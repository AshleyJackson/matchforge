import eslint from "@eslint/js";
import babelParser from "@babel/eslint-parser";

export default [
  {
    ignores: ["dist/**", "node_modules/**", ".tsup/**"],
  },
  eslint.configs.recommended,
  {
    files: ["src/**/*.ts", "tests/**/*.ts"],
    languageOptions: {
      parser: babelParser,
      parserOptions: {
        requireConfigFile: false,
        babelOptions: {
          plugins: ["@babel/plugin-syntax-typescript"],
        },
      },
    },
    rules: {
      "no-undef": "off",
      "no-unused-vars": "off",
    },
  },
];
