# Contributing Guidelines

Thank you for your interest in the Pain project! We warmly welcome community contributions and are committed to providing a friendly and inclusive environment for all contributors.

## Prerequisites

Before you begin contributing, please ensure that you have:

1. Read and understood the project's [README.md](https://github.com/ivanallen/pain/blob/main/README.md)
2. Configured your development environment according to the instructions
3. Familiarized yourself with the project's basic architecture and coding style

## Development Environment Setup

Please follow the detailed instructions in [README.md](https://github.com/ivanallen/pain/blob/main/README.md) to configure your development environment, including necessary dependencies and toolchains.

## Code Quality Requirements

### 1. Test Coverage

All new features and modifications must include corresponding test cases. Before submitting code, please ensure:

```bash
# Run all tests
./z.py t

# Ensure all tests pass
```

**Important**: If your modifications cause test failures, please fix the test issues before submitting the code.

### 2. Code Formatting

The project uses unified code formatting standards. Before submitting code, please run the following command to automatically format your code:

```bash
# Automatically format code
./z.py f
```

### 3. Code Quality Inspection

The project uses clang-tidy for code quality inspection. Please ensure your code passes all lint checks:

```bash
# Generate compilation database (lint checks depend on this file)
./tools/compile_commands.sh all

# Run lint checks
./z.py lint
```

If lint checks identify issues, the system will clearly indicate the problem location and type. We recommend that you:

- Carefully read the error messages
- Fix code issues according to the suggestions
- Re-run lint checks until they pass

## Development Tools Recommendations

### IDE Configuration

For the best development experience, we recommend:

1. **Install clangd plugin**: If you use VS Code or other IDEs that support LSP, we strongly recommend installing the clangd plugin
2. **Configure clangd**: The plugin will automatically provide code completion, error prompts, and formatting suggestions
3. **Real-time feedback**: The IDE will display code issues in real-time, helping you identify and fix problems promptly

### Recommended Development Tools

- **VS Code** + clangd plugin
- **CLion** (built-in clang-tidy support)
- **Vim/Neovim** + clangd plugin
- **Emacs** + clangd plugin

## Submission Standards

### Commit Message Format

Please use clear commit message formats:

```
<type>(<scope>): <short description>

<detailed description (optional)>

<related issue links (optional)>
```

**Type Examples**:
- `feat`: New feature
- `fix`: Bug fix
- `docs`: Documentation update
- `style`: Code formatting adjustment
- `refactor`: Code refactoring
- `test`: Test-related changes
- `chore`: Build process or auxiliary tool changes

### Code Review

All code submissions require code review. Please ensure:

1. Code complies with project standards
2. Includes necessary test cases
3. Passes all quality checks
4. Provides clear submission descriptions

## Issue Reporting

If you discover bugs or have feature suggestions, please:

1. Search GitHub Issues to see if similar issues already exist
2. If none exist, create a new issue
3. Provide detailed reproduction steps and environment information
4. Use clear titles and descriptions

## Feature Requests

For new feature requests, please:

1. Describe the feature requirements and use cases in detail
2. Explain why this feature is needed
3. Provide possible implementation approaches (if applicable)
4. Discuss the impact on existing functionality

## Community Code of Conduct

We are committed to maintaining a friendly and inclusive development community. Please:

- Respect all contributors
- Use constructive language in discussions
- Welcome new contributors
- Provide constructive feedback

## Getting Help

If you encounter problems during the contribution process:

1. Review project documentation
2. Search existing issues and discussions
3. Ask questions in GitHub Discussions
4. Contact project maintainers

## Acknowledgments

Thank you again for your contribution to the Pain project! Your participation is crucial to the project's development.

---

**Note**: This guide will be updated as the project evolves. If you find any issues in the guide or have improvement suggestions, please feel free to create an issue or pull request.
