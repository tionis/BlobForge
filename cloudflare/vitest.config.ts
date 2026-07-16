import { defineWorkersConfig } from "@cloudflare/vitest-pool-workers/config";

export default defineWorkersConfig({
  test: {
    poolOptions: {
      workers: {
        isolatedStorage: false,
        wrangler: { configPath: "./wrangler.jsonc" },
        miniflare: {
          bindings: {
            WORKER_API_TOKEN: "test-worker-token",
            MIGRATION_API_TOKEN: "test-migration-token",
            ADMIN_ME: "https://eric.wendland.dev/",
          },
        },
      },
    },
  },
});
