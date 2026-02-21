# frozen_string_literal: true

# name: discourse-digest-campaigns2
# about: Admin-defined digest campaigns from a SQL segment + up to 3 random topic sets. Populate once on create; optional scheduled send_at; throttled batched sending; admin UI.
# version: 1.0.0
# authors: you
# required_version: 3.0.0

enabled_site_setting :digest_campaigns_enabled

# This adds the plugin entry under /admin/plugins (label from client locale: js.digest_campaigns.title)
add_admin_route "digest_campaigns.title", "digest-campaigns"

after_initialize do
  # ============================================================================
  # Core helpers / constants
  # ============================================================================
  module ::DigestCampaigns
    PLUGIN_NAME = "discourse-digest-campaigns2"
    QUEUE_TABLE = "digest_campaign_queue"
    CAMPAIGNS_TABLE = "digest_campaigns"

    def self.minute_bucket_key
      (Time.now.utc.to_i / 60).to_i
    end

    def self.redis_rate_key(bucket)
      "digest_campaigns:sent:#{bucket}"
    end

    def self.validate_campaign_sql!(sql)
      s = sql.to_s.strip
      raise ArgumentError, "campaign SQL is blank" if s.empty?
      raise ArgumentError, "campaign SQL must NOT contain semicolons" if s.include?(";")
      unless s.match?(/\A(select|with)\b/i)
        raise ArgumentError, "campaign SQL must start with SELECT or WITH"
      end
      s
    end

    def self.parse_topic_set_csv(csv)
      s = csv.to_s.strip
      return [] if s.blank?
      s.split(",").map { |x| x.strip }.reject(&:blank?).map(&:to_i).select { |n| n > 0 }
    end

    def self.pick_random_topic_set(topic_sets)
      sets =
        Array(topic_sets)
          .map { |a| Array(a).map(&:to_i).select { |n| n > 0 } }
          .reject(&:blank?)
      return [] if sets.empty?
      sets[SecureRandom.random_number(sets.length)]
    end
  end

  # ============================================================================
  # Model
  # ============================================================================
  module ::DigestCampaigns
    class Campaign < ActiveRecord::Base
      self.table_name = ::DigestCampaigns::CAMPAIGNS_TABLE

      validates :campaign_key, presence: true, uniqueness: true
      validates :selection_sql, presence: true
    end
  end

  # ============================================================================
  # UserNotifications digest override (campaign support)
  # IMPORTANT:
  # - Use prepend (not include) so our digest method actually overrides core.
  # - Accept keyword args so campaign_topic_ids: works reliably.
  # ============================================================================
  require_dependency "email/sender"
  require_dependency "email/message_builder"

  module ::DigestCampaigns
    module UserNotificationsExtension
      def digest(user, opts = nil, **kwargs)
        opts = (opts || {}).dup
        opts.merge!(kwargs) if kwargs.present?

        campaign_topic_ids = opts[:campaign_topic_ids]
        campaign_key = opts[:campaign_key]
        campaign_since = opts[:campaign_since]

        # Normal digests unaffected
        if campaign_topic_ids.blank?
          return digest_without_campaigns(user, opts)
        end

        build_summary_for(user)

        @campaign_key = campaign_key.to_s
        @unsubscribe_key = UnsubscribeKey.create_key_for(@user, UnsubscribeKey::DIGEST_TYPE)
        @since = campaign_since.presence || [user.last_seen_at, 1.month.ago].compact.max

        ids = Array(campaign_topic_ids).map(&:to_i).select { |x| x > 0 }.uniq
        topics = Topic.where(id: ids).includes(:category, :user, :first_post).to_a
        by_id = topics.index_by(&:id)
        topics_for_digest = ids.map { |id| by_id[id] }.compact

        # Use the site's configured digest_topics as the split point.
        popular_n = SiteSetting.digest_topics.to_i
        popular_n = 0 if popular_n < 0
        popular_n = 1 if popular_n == 0 && topics_for_digest.present?

        @popular_topics = topics_for_digest[0, popular_n] || []
        @other_new_for_you =
          if topics_for_digest.size > popular_n
            topics_for_digest[popular_n..-1] || []
          else
            []
          end

        # Campaign: don't add extra content (you said forget posts for now)
        @popular_posts = []

        # Excerpts map (used by digest template)
        @excerpts = {}
        @popular_topics.each do |t|
          next if t&.first_post.blank?
          next if t.first_post.user_deleted
          @excerpts[t.first_post.id] = email_excerpt(t.first_post.cooked, t.first_post)
        end

        # Minimal counts block (digest template expects @counts)
        @counts = [
          {
            id: "new_topics",
            label_key: "user_notifications.digest.new_topics",
            value: topics_for_digest.size,
            href: "#{Discourse.base_url}/new",
          },
        ]

        @preheader_text = I18n.t("user_notifications.digest.preheader", since: @since)

        base_subject =
          I18n.t(
            "user_notifications.digest.subject_template",
            email_prefix: @email_prefix,
            date: short_date(Time.now)
          )

        prefix = SiteSetting.digest_campaigns_subject_prefix.to_s.strip
        prefix = "[Campaign Digest]" if prefix.blank?
        subject = "#{prefix} - #{base_subject} - #{@campaign_key}".strip

        # IMPORTANT: render the REAL digest template so downstream digest-processing plugins can hook in.
        html = render_to_string(template: "user_notifications/digest", formats: [:html])

        # Plain-text fallback (avoids missing text template errors)
        lines = []
        lines << "Activity Summary"
        lines << "Campaign: #{@campaign_key}" if @campaign_key.present?
        lines << ""
        if topics_for_digest.empty?
          lines << "(No topics)"
        else
          lines << "Topics:"
          topics_for_digest.each_with_index do |t, i|
            lines << "#{i + 1}. #{t.title} - #{Discourse.base_url}/t/#{t.slug}/#{t.id}"
          end
        end
        lines << ""
        lines << "Unsubscribe: #{Discourse.base_url}/email/unsubscribe/#{@unsubscribe_key}"
        text_body = lines.join("\n")

        build_email(
          user.email,
          subject: subject,
          body: text_body,
          html_override: html,
          add_unsubscribe_link: true,
          unsubscribe_url: "#{Discourse.base_url}/email/unsubscribe/#{@unsubscribe_key}",
          topic_ids: topics_for_digest.map(&:id),
          post_ids: topics_for_digest.map { |t| t.first_post&.id }.compact
        )
      end
    end
  end

  ::UserNotifications.class_eval do
    unless method_defined?(:digest_without_campaigns)
      alias_method :digest_without_campaigns, :digest
    end
    prepend ::DigestCampaigns::UserNotificationsExtension
  end

  # ============================================================================
  # Jobs
  # ============================================================================
  module ::Jobs
    class DigestCampaignPoller < ::Jobs::Scheduled
      every 1.minute
      sidekiq_options queue: "default"

      def execute(_args)
        return unless SiteSetting.digest_campaigns_enabled

        every_n = SiteSetting.digest_campaigns_poller_every_minutes.to_i
        every_n = 1 if every_n <= 0
        bucket = ::DigestCampaigns.minute_bucket_key
        return if (bucket % every_n) != 0

        only_key = SiteSetting.digest_campaigns_only_campaign_key.to_s.strip
        stale_minutes = SiteSetting.digest_campaigns_processing_stale_minutes.to_i
        claim_rows = SiteSetting.digest_campaigns_claim_rows_per_run.to_i
        chunk_size = SiteSetting.digest_campaigns_batch_chunk_size.to_i
        chunk_size = 25 if chunk_size <= 0

        requeue_stale(stale_minutes)
        claim_and_enqueue(only_key, claim_rows, chunk_size)
      end

      private

      def requeue_stale(stale_minutes)
        stale_minutes = 30 if stale_minutes <= 0
        cutoff = Time.now.utc - stale_minutes.minutes

        DB.exec(<<~SQL, cutoff)
          UPDATE #{::DigestCampaigns::QUEUE_TABLE}
             SET status='queued',
                 note='Re-queued stale processing row',
                 updated_at=NOW()
           WHERE status='processing'
             AND updated_at < ?
        SQL
      end

      def claim_and_enqueue(only_key, claim_rows, chunk_size)
        claim_rows = 200 if claim_rows <= 0

        rows = DB.query(<<~SQL, only_key, only_key, claim_rows)
          SELECT id
            FROM #{::DigestCampaigns::QUEUE_TABLE}
           WHERE status='queued'
             AND (send_at IS NULL OR send_at <= NOW())
             AND (? = '' OR campaign_key = ?)
           ORDER BY id ASC
           LIMIT ?
        SQL

        ids = rows.map { |r| r["id"].to_i }.select { |x| x > 0 }
        return if ids.empty?

        DB.exec(<<~SQL, ids)
          UPDATE #{::DigestCampaigns::QUEUE_TABLE}
             SET status='processing',
                 note='Claimed by poller',
                 updated_at=NOW()
           WHERE id IN (?)
             AND status='queued'
        SQL

        ids.each_slice(chunk_size) do |slice|
          ::Jobs.enqueue(:digest_campaign_send_batch, queue_ids: slice)
        end
      end
    end

    class DigestCampaignSendBatch < ::Jobs::Base
      sidekiq_options queue: "default"

      def execute(args)
        return unless SiteSetting.digest_campaigns_enabled

        queue_ids = Array(args[:queue_ids]).map(&:to_i).select { |x| x > 0 }
        return if queue_ids.empty?

        target_per_min = SiteSetting.digest_campaigns_target_per_minute.to_i
        target_per_min = 1 if target_per_min <= 0

        bucket = ::DigestCampaigns.minute_bucket_key
        rate_key = ::DigestCampaigns.redis_rate_key(bucket)
        Discourse.redis.expire(rate_key, 120)

        queue_ids.each_with_index do |qid, idx|
          sent_this_min = Discourse.redis.get(rate_key).to_i
          if sent_this_min >= target_per_min
            remaining = queue_ids[idx..-1]
            requeue_rows(remaining, "Throttled: hit #{target_per_min}/min")
            return
          end

          row = DB.query_single(<<~SQL, qid)
            SELECT *
              FROM #{::DigestCampaigns::QUEUE_TABLE}
             WHERE id = ?
             LIMIT 1
          SQL

          next if row.blank?
          next if row["status"].to_s != "processing"

          begin
            handle_row(row)
            Discourse.redis.incr(rate_key)
          rescue => e
            fail_row(qid, "Exception: #{e.class}: #{e.message}".truncate(500))
            Rails.logger.error("[digest-campaigns2] send_batch row_id=#{qid} error=#{e.class} #{e.message}\n#{e.backtrace&.join("\n")}")
          end
        end
      end

      private

      def handle_row(row)
        qid = row["id"].to_i
        user_id = row["user_id"].to_i
        email = row["user_email"].to_s
        campaign_key = row["campaign_key"].to_s
        topic_ids = ::DigestCampaigns.parse_topic_set_csv(row["chosen_topic_ids"])

        user = User.find_by(id: user_id)
        if user.blank?
          fail_row(qid, "User not found")
          return
        end

        if user.email.to_s != email
          fail_row(qid, "Email mismatch")
          return
        end

        if user.user_option&.digest_after_minutes.to_i <= 0
          skip_row(qid, "skipped_unsubscribed", "User digest disabled")
          return
        end

        message = ::UserNotifications.digest(
          user,
          campaign_topic_ids: topic_ids,
          campaign_key: campaign_key,
          campaign_since: Time.zone.now
        )

        if message.blank?
          fail_row(qid, "Digest returned nil message")
          return
        end

        Email::Sender.new(message, :digest).send

        DB.exec(<<~SQL, qid)
          UPDATE #{::DigestCampaigns::QUEUE_TABLE}
             SET status='sent',
                 note='Sent',
                 updated_at=NOW()
           WHERE id = ?
        SQL
      end

      def requeue_rows(ids, note)
        return if ids.blank?
        DB.exec(<<~SQL, note.to_s.truncate(500), ids)
          UPDATE #{::DigestCampaigns::QUEUE_TABLE}
             SET status='queued',
                 note=?,
                 updated_at=NOW()
           WHERE id IN (?)
             AND status='processing'
        SQL
      end

      def fail_row(id, note)
        DB.exec(<<~SQL, note.to_s.truncate(500), id)
          UPDATE #{::DigestCampaigns::QUEUE_TABLE}
             SET status='failed',
                 note=?,
                 updated_at=NOW()
           WHERE id = ?
        SQL
      end

      def skip_row(id, status, note)
        DB.exec(<<~SQL, status.to_s, note.to_s.truncate(500), id)
          UPDATE #{::DigestCampaigns::QUEUE_TABLE}
             SET status=?,
                 note=?,
                 updated_at=NOW()
           WHERE id = ?
        SQL
      end
    end
  end

  # ============================================================================
  # Admin controller + routes
  # ============================================================================
  module ::Admin
    class DigestCampaignsController < ::Admin::AdminController
      requires_plugin ::DigestCampaigns::PLUGIN_NAME

      def index
        rows = ::DigestCampaigns::Campaign.order("created_at DESC").limit(200).map do |c|
          c.as_json.merge(
            queued_count: queue_count(c.campaign_key, "queued"),
            processing_count: queue_count(c.campaign_key, "processing"),
            sent_count: queue_count(c.campaign_key, "sent"),
            failed_count: queue_count(c.campaign_key, "failed"),
            skipped_unsubscribed_count: queue_count(c.campaign_key, "skipped_unsubscribed")
          )
        end
        render_json_dump(campaigns: rows)
      end

      def create
        key = params.require(:campaign_key).to_s.strip
        sql = ::DigestCampaigns.validate_campaign_sql!(params.require(:selection_sql).to_s)

        set1 = ::DigestCampaigns.parse_topic_set_csv(params[:topic_set_1])
        set2 = ::DigestCampaigns.parse_topic_set_csv(params[:topic_set_2])
        set3 = ::DigestCampaigns.parse_topic_set_csv(params[:topic_set_3])
        topic_sets = [set1, set2, set3].reject(&:blank?)
        raise ArgumentError, "You must provide at least one topic set (topic_set_1..3)" if topic_sets.blank?

        send_at = params[:send_at].presence
        send_at = Time.zone.parse(send_at.to_s) if send_at

        c = ::DigestCampaigns::Campaign.new(
          campaign_key: key,
          selection_sql: sql,
          topic_sets_json: topic_sets.to_json,
          enabled: true,
          send_at: send_at
        )
        c.save!

        populate_queue!(c)

        render_json_dump(ok: true, campaign: c.as_json)
      end

      def enable
        c = find_campaign
        c.update!(enabled: true)
        render_json_dump(ok: true)
      end

      def disable
        c = find_campaign
        c.update!(enabled: false)
        render_json_dump(ok: true)
      end

      def destroy
        c = find_campaign
        DB.exec("DELETE FROM #{::DigestCampaigns::QUEUE_TABLE} WHERE campaign_key = ?", c.campaign_key.to_s)
        c.destroy!
        render_json_dump(ok: true)
      end

      def test_send
        c = find_campaign
        email = params.require(:email).to_s.strip
        user = User.find_by(email: email)
        raise Discourse::NotFound, "User not found for #{email}" if user.blank?

        topic_sets = JSON.parse(c.topic_sets_json.to_s) rescue []
        chosen = ::DigestCampaigns.pick_random_topic_set(topic_sets)

        message = ::UserNotifications.digest(
          user,
          campaign_topic_ids: chosen,
          campaign_key: c.campaign_key,
          campaign_since: Time.zone.now
        )
        Email::Sender.new(message, :digest).send

        render_json_dump(ok: true, chosen_topic_ids: chosen)
      end

      private

      def find_campaign
        id = params.require(:id).to_i
        c = ::DigestCampaigns::Campaign.find_by(id: id)
        raise Discourse::NotFound if c.blank?
        c
      end

      def queue_count(campaign_key, status)
        DB.query_single(<<~SQL, campaign_key.to_s, status.to_s)["c"].to_i
          SELECT COUNT(*) AS c
            FROM #{::DigestCampaigns::QUEUE_TABLE}
           WHERE campaign_key = ?
             AND status = ?
        SQL
      end

      def populate_queue!(campaign)
        topic_sets = JSON.parse(campaign.topic_sets_json.to_s) rescue []
        chosen_topic_ids = ::DigestCampaigns.pick_random_topic_set(topic_sets)
        chosen_csv = chosen_topic_ids.join(",")

        rows = DB.query(campaign.selection_sql)

        now = Time.now.utc
        rows.each do |r|
          uid = r["user_id"].to_i
          em = r["email"].to_s.strip
          next if uid <= 0 || em.blank?

          DB.exec(<<~SQL, campaign.campaign_key.to_s, uid, em, chosen_csv, campaign.send_at, now, now)
            INSERT INTO #{::DigestCampaigns::QUEUE_TABLE}
              (campaign_key, user_id, user_email, chosen_topic_ids, status, send_at, note, created_at, updated_at)
            VALUES
              (?, ?, ?, ?, 'queued', ?, 'Queued by create', ?, ?)
          SQL
        end
      end
    end
  end

  Discourse::Application.routes.append do
    # Admin UI entry (supported plugin-admin pattern)
    get "/admin/plugins/digest-campaigns" => "admin/plugins#index", constraints: StaffConstraint.new
    # Convenience redirect
    get "/admin/digest-campaigns" => redirect("/admin/plugins/digest-campaigns"), constraints: StaffConstraint.new

    namespace :admin do
      get    "/digest-campaigns.json" => "digest_campaigns#index"
      post   "/digest-campaigns.json" => "digest_campaigns#create"
      put    "/digest-campaigns/:id/enable.json" => "digest_campaigns#enable"
      put    "/digest-campaigns/:id/disable.json" => "digest_campaigns#disable"
      post   "/digest-campaigns/:id/test.json" => "digest_campaigns#test_send"
      delete "/digest-campaigns/:id.json" => "digest_campaigns#destroy"
    end
  end
end
