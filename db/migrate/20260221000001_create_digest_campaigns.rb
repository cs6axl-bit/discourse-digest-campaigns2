# frozen_string_literal: true

class CreateDigestCampaigns < ActiveRecord::Migration[7.0]
  def up
    return if table_exists?(:digest_campaigns)

    create_table :digest_campaigns do |t|
      t.text :campaign_key, null: false
      t.text :selection_sql, null: false
      t.boolean :enabled, null: false, default: true

      # JSONB like: [[1,2,3],[4,5],[6]]
      t.jsonb :topic_sets, null: false, default: []

      # Optional scheduled send time (queue rows use this as not_before)
      t.datetime :send_at

      t.text :last_error
      t.datetime :last_populated_at

      t.timestamps null: false
    end

    add_index :digest_campaigns, :campaign_key, unique: true, name: "uidx_dcamp_key"
    add_index :digest_campaigns, :enabled, name: "idx_dcamp_enabled"
    add_index :digest_campaigns, :send_at, name: "idx_dcamp_send_at"
  end

  def down
    drop_table :digest_campaigns if table_exists?(:digest_campaigns)
  end
end
