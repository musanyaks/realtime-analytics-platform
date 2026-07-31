# Contributing to Real-Time Analytics Platform

## Git Workflow

We use a simple trunk-based workflow:

```bash
# 1. Clone the repo
git clone https://github.com/YOUR_USERNAME/realtime-analytics-platform.git
cd realtime-analytics-platform

# 2. Create a feature branch
git checkout -b feature/your-feature-name

# 3. Make changes, commit often
git add .
git commit -m "feat: add kafka partitioning strategy"

# 4. Push and open PR
git push origin feature/your-feature-name
```

## Commit Convention

- `feat:` — New feature
- `fix:` — Bug fix
- `docs:` — Documentation only
- `test:` — Adding tests
- `refactor:` — Code refactoring
- `chore:` — Maintenance tasks

## Pre-commit Checklist

- [ ] `make lint-r` passes
- [ ] `make test` passes
- [ ] `dbt compile` succeeds
- [ ] `.env` is NOT committed
- [ ] README updated if needed
