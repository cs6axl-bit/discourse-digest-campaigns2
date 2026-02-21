# frozen_string_literal: true

class CreateDigestCampaignQueue < ActiveRecord::Migration[7.0]
  def up
    return if table_exists?(:digest_campaign_queue)

    create_table :digest_campaign_queue do |t|
      t.text    :campaign_key, null: false, default: ""
      t.integer :user_id, null: false

      # Chosen randomly at send-time (once). Retries reuse this choice.
      t.integer :chosen_topic_ids, array: true, null: false, default: []

      # Do not send before this timestamp (NULL = send anytime)
      t.datetime :not_before

      # queued | processing | sent | failed | skipped_unsubscribed
      t.text :status, null: false, default: "queued"

      t.datetime :locked_at
      t.integer  :attempts, null: false, default: 0
      t.text     :last_error
      t.datetime :sent_at

      t.timestamps null: false
    end

    add_index :digest_campaign_queue, [:status, :campaign_key, :id], name: "idx_dcq_status_campaign_id"
    add_index :digest_campaign_queue, [:campaign_key, :user_id], unique: true, name: "uidx_dcq_campaign_user"
    add_index :digest_campaign_queue, :locked_at, name: "idx_dcq_locked_at"
    add_index :digest_campaign_queue, :not_before, name: "idx_dcq_not_before"
  end

  def down
    drop_table :digest_campaign_queue if table_exists?(:digest_campaign_queue)
  end
end
