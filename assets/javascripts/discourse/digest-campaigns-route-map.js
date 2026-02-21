export default {
  resource: "admin.adminPlugins",
  path: "/plugins",

  map() {
    this.route("digest-campaigns", { path: "digest-campaigns" });
  },
};
