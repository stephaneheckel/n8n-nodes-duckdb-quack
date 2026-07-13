import { configWithoutCloudSupport } from '@n8n/node-cli/eslint';

export default [
  ...configWithoutCloudSupport,
  {
    rules: {
      // @duckdb/node-api is a native module that must be a runtime dependency.
      // Pure-JS library conflict concerns don't apply to platform-specific binaries.
      '@n8n/community-nodes/no-runtime-dependencies': 'off',
      // DuckDB is a native C++ engine — no HTTP health endpoint to test against.
      // Credential testing via ICredentialTestRequest is not applicable.
      '@n8n/community-nodes/credential-test-required': 'off',
    },
  },
];
