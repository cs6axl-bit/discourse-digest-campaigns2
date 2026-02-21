import Controller from "@ember/controller";
import { action } from "@ember/object";
import { tracked } from "@glimmer/tracking";
import { ajax } from "discourse/lib/ajax";

export default class AdminPluginsDigestCampaignsController extends Controller {
  @tracked campaigns = [];

  @tracked campaign_key = "";
  @tracked selection_sql = "";
  @tracked topic_set_1 = "";
  @tracked topic_set_2 = "";
  @tracked topic_set_3 = "";
  @tracked send_at = ""; // datetime-local
  @tracked test_email = "";

  @tracked busy = false;
  @tracked error = "";
  @tracked notice = "";

  @tracked testEmailById = {};

  clearMessages() {
    this.error = "";
    this.notice = "";
  }

  async refresh() {
    const res = await ajax("/admin/digest-campaigns.json");
    this.campaigns = res.campaigns || [];
  }

  @action
  onTestEmailInput(id, event) {
    const value = event?.target?.value || "";
    this.testEmailById = { ...this.testEmailById, [id]: value };
  }

  @action
  async refreshNow() {
    this.clearMessages();
    this.busy = true;
    try {
      await this.refresh();
      this.notice = "Refreshed.";
    } catch (e) {
      this.error = e?.message || "Refresh failed";
    } finally {
      this.busy = false;
    }
  }

  @action
  async createCampaign() {
    this.clearMessages();
    this.busy = true;

    try {
      const payload = {
        campaign_key: this.campaign_key,
        selection_sql: this.selection_sql,
        topic_set_1: this.topic_set_1,
        topic_set_2: this.topic_set_2,
        topic_set_3: this.topic_set_3,
        test_email: this.test_email,
      };

      if (this.send_at && this.send_at.trim().length > 0) {
        const d = new Date(this.send_at);
        payload.send_at = d.toISOString();
      }

      await ajax("/admin/digest-campaigns.json", { type: "POST", data: payload });

      this.notice = "Campaign created and queue populated.";
      this.campaign_key = "";
      this.selection_sql = "";
      this.topic_set_1 = "";
      this.topic_set_2 = "";
      this.topic_set_3 = "";
      this.send_at = "";
      this.test_email = "";

      await this.refresh();
    } catch (e) {
      this.error =
        e?.jqXHR?.responseJSON?.errors?.[0] ||
        e?.message ||
        "Failed to create campaign";
    } finally {
      this.busy = false;
    }
  }

  @action
  async enableCampaign(id) {
    this.clearMessages();
    this.busy = true;
    try {
      await ajax(`/admin/digest-campaigns/${id}/enable.json`, { type: "PUT" });
      this.notice = "Enabled.";
      await this.refresh();
    } catch (e) {
      this.error =
        e?.jqXHR?.responseJSON?.errors?.[0] || e?.message || "Enable failed";
    } finally {
      this.busy = false;
    }
  }

  @action
  async disableCampaign(id) {
    this.clearMessages();
    this.busy = true;
    try {
      await ajax(`/admin/digest-campaigns/${id}/disable.json`, { type: "PUT" });
      this.notice = "Disabled.";
      await this.refresh();
    } catch (e) {
      this.error =
        e?.jqXHR?.responseJSON?.errors?.[0] || e?.message || "Disable failed";
    } finally {
      this.busy = false;
    }
  }

  @action
  async deleteCampaign(id) {
    this.clearMessages();
    if (
      !confirm(
        "Delete this campaign? (Queue rows remain unless you remove them manually.)"
      )
    ) {
      return;
    }

    this.busy = true;
    try {
      await ajax(`/admin/digest-campaigns/${id}.json`, { type: "DELETE" });
      this.notice = "Deleted.";
      await this.refresh();
    } catch (e) {
      this.error =
        e?.jqXHR?.responseJSON?.errors?.[0] || e?.message || "Delete failed";
    } finally {
      this.busy = false;
    }
  }

  @action
  async testSend(id) {
    this.clearMessages();
    const email = (this.testEmailById?.[id] || "").trim();
    if (!email) {
      this.error = "Enter a test email for this campaign.";
      return;
    }

    this.busy = true;
    try {
      await ajax(`/admin/digest-campaigns/${id}/test.json`, {
        type: "POST",
        data: { test_email: email },
      });
      this.notice = `Test sent to ${email}`;
    } catch (e) {
      this.error =
        e?.jqXHR?.responseJSON?.errors?.[0] ||
        e?.message ||
        "Test send failed";
    } finally {
      this.busy = false;
    }
  }
}
