module.exports = {
  root: true,
  env: {
    node: true,
  },
  extends: [
  //
    'eslint:recommended',
    'plugin:@typescript-eslint/recommended',
    'prettier',
  ],
  parser: '@typescript-eslint/parser',
}
