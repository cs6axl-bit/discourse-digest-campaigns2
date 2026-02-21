import Route from "@ember/routing/route";
import { ajax } from "discourse/lib/ajax";

export default class AdminPluginsDigestCampaignsRoute extends Route {
  async model() {
    return await ajax("/admin/digest-campaigns.json");
  }

  setupController(controller, model) {
    super.setupController(controller, model);
    controller.campaigns = model?.campaigns || [];
  }
}
