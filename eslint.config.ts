// @ts-check

import * as eslint from "@eslint/js";
import * as tseslint from "typescript-eslint";
import { defineConfig, globalIgnores } from "eslint/config";

export default defineConfig(
  eslint.configs.recommended,
  tseslint.configs.strict,
  tseslint.configs.stylistic,
  globalIgnores(["**/dist/**", "tmp/**"]),
  {
    linterOptions: {
      reportUnusedDisableDirectives: "warn",
    },
    rules: {
      "no-console": "error",
      "@typescript-eslint/no-unused-expressions": [
        "error",
        { allowTaggedTemplates: true },
      ],
      "@typescript-eslint/consistent-type-definitions": 0,
      "@typescript-eslint/no-unsafe-type-assertion": "error",
      "no-restricted-syntax": [
        "error",
        {
          selector:
            "CallExpression[callee.name='useParams'] Property[key.name='strict'][value.value=false]",
          message:
            "Do not use useParams({ strict: false }). Use useParams({ from: '/path/$param' }) for typed route params.",
        },
      ],
      "@typescript-eslint/no-unused-vars": [
        "error",
        {
          args: "all",
          argsIgnorePattern: "^_",
          caughtErrors: "all",
          caughtErrorsIgnorePattern: "^_",
          destructuredArrayIgnorePattern: "^_",
          varsIgnorePattern: "^_",
          ignoreRestSiblings: true,
        },
      ],
    },
    languageOptions: {
      parserOptions: {
        projectService: {
          allowDefaultProject: [
            "eslint.config.ts",
            "apps/admin-ui/vite.config.ts",
            "packages/db/drizzle.config.ts",
            "tests/inference/transform-cutover.test.ts",
            "tests/agent/*.ts",
            "tests/coding-agent/*.ts",
            "tests/agent-common/*.ts",
            "tests/agent-quickstart/*.ts",
            "tests/agent-resume/*.ts",
            "tests/agent-rewind/*.ts",
            "tests/agent-blob-spill/*.ts",
            "tests/agent-audit-log/*.ts",
            "tests/agent-multi-provider/*.ts",
            "tests/agent-rich-tool/*.ts",
            "tests/agent-structured-payload/*.ts",
            "tests/workflow/*.ts",
            "tests/workflow-deploy/*.ts",
          ],
          maximumDefaultProjectFileMatchCount_THIS_WILL_SLOW_DOWN_LINTING: 27,
        },
        tsconfigRootDir: import.meta.dirname,
      },
    },
  },
);
