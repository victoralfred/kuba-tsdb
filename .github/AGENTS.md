# Two-Agent Development Workflow

This repository uses a two-agent AI workflow for development and code review.

## Agent Roles

### 1. Main Agent (Claude Code CLI)

**Role**: Development and Implementation

**Responsibilities**:
- Write new features and bug fixes
- Create pull requests
- Respond to review feedback
- Commit and push changes

**Usage**:
```bash
# Start Claude Code in the repository
claude

# Example tasks:
# "Implement a new compression codec"
# "Fix the memory leak in the cache module"
# "Add rate limiting to the query engine"
```

### 2. Review Agent (GitHub Actions)

**Role**: Automated Code Review

**Responsibilities**:
- Review all pull requests automatically
- Check for security vulnerabilities
- Identify performance issues
- Suggest improvements
- Provide verdict (APPROVE / REQUEST_CHANGES / COMMENT)

**Triggers**:
- Pull request opened
- Pull request synchronized (new commits)
- Pull request reopened

## Workflow Diagram

```
┌─────────────────────────────────────────────────────────────────┐
│                     Development Workflow                         │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  ┌──────────────┐     ┌──────────────┐     ┌──────────────┐    │
│  │  Developer   │────▶│  Main Agent  │────▶│  Create PR   │    │
│  │  (Request)   │     │ (Claude Code)│     │              │    │
│  └──────────────┘     └──────────────┘     └──────┬───────┘    │
│                                                    │             │
│                                                    ▼             │
│  ┌──────────────┐     ┌──────────────┐     ┌──────────────┐    │
│  │    Merge     │◀────│ Review Agent │◀────│  GitHub      │    │
│  │              │     │  (Claude AI) │     │  Actions     │    │
│  └──────────────┘     └──────────────┘     └──────────────┘    │
│         ▲                    │                                   │
│         │                    ▼                                   │
│         │             ┌──────────────┐                          │
│         │             │   Verdict    │                          │
│         │             │ ┌──────────┐ │                          │
│         └─────────────│ │ APPROVE  │ │                          │
│                       │ └──────────┘ │                          │
│                       │ ┌──────────┐ │                          │
│              ┌────────│ │ CHANGES  │ │                          │
│              │        │ └──────────┘ │                          │
│              │        └──────────────┘                          │
│              ▼                                                   │
│  ┌──────────────┐                                               │
│  │  Main Agent  │ (Address feedback)                            │
│  │  (Revise)    │                                               │
│  └──────────────┘                                               │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

## Setup Requirements

### 1. GitHub Secrets

Add the following secret to your repository:

```
ANTHROPIC_API_KEY: Your Anthropic API key
```

Go to: Repository Settings → Secrets and variables → Actions → New repository secret

### 2. Workflow Permissions

Ensure the workflow has permissions to:
- Read repository contents
- Write pull request comments

Go to: Repository Settings → Actions → General → Workflow permissions → Read and write permissions

## Review Criteria

The Review Agent evaluates PRs based on:

| Category | Focus Areas |
|----------|-------------|
| **Security** | Buffer overflows, integer overflow, unsafe code, injection vulnerabilities |
| **Performance** | Unnecessary allocations, lock contention, algorithmic complexity |
| **Correctness** | Edge cases, error handling, race conditions |
| **Code Quality** | Readability, documentation, idiomatic Rust |
| **Testing** | Test coverage, edge case testing |

## Review Output Format

```markdown
### Summary
Brief description of what the PR does

### Security Concerns
- Issue 1 (file:line)
- Issue 2 (file:line)
- None found

### Performance Considerations
- Consideration 1
- Looks good

### Suggested Improvements
1. Specific suggestion with file:line reference
2. Another suggestion

### Verdict
APPROVE / REQUEST_CHANGES / COMMENT
Brief justification
```

## Best Practices

### For the Main Agent

1. **Create focused PRs**: One feature or fix per PR
2. **Write descriptive commits**: Clear commit messages
3. **Run checks locally**: `cargo clippy && cargo test` before pushing
4. **Address feedback promptly**: Fix issues raised by Review Agent

### For Review Agent Configuration

1. **Diff size limit**: Large diffs are truncated to 50KB to prevent token overflow
2. **Model selection**: Uses Claude Sonnet for balanced speed/quality
3. **Retry on failure**: Consider adding retry logic for API failures

## Customization

### Modify Review Focus

Edit `.github/workflows/pr-review.yml` and update the review prompt to:
- Add project-specific guidelines
- Emphasize certain review criteria
- Include architectural constraints

### Change Review Model

Update the `model` field in the workflow:
```json
"model": "claude-sonnet-4-20250514"  // or claude-opus-4-20250514 for deeper review
```

### Add Branch Protection

Recommended branch protection rules:
- Require PR reviews before merging
- Require status checks (build, test, clippy, ai-review)
- Dismiss stale reviews on new commits

## Troubleshooting

### Review Not Posting

1. Check `ANTHROPIC_API_KEY` secret is set
2. Verify workflow permissions
3. Check Actions logs for API errors

### Review Quality Issues

1. Ensure diff is not truncated excessively
2. Consider using a more capable model
3. Refine the review prompt for your project

### Rate Limiting

If hitting API rate limits:
1. Add delays between API calls
2. Cache reviews for unchanged files
3. Consider using batch reviews for large PRs
