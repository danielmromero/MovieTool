# Daniel's Movie Tool — clean repo package

Upload **all files in this folder** to the root of your GitHub repo, including hidden items:
- `.github/workflows/refresh_catalog.yml`
- `.python-version`

## Render environment variables
Set these in Render:
- `GITHUB_REFRESH_REPO=danielmromero/MovieTool`
- `GITHUB_REFRESH_WORKFLOW=refresh_catalog.yml`
- `GITHUB_REFRESH_REF=main`
- `GITHUB_REFRESH_TOKEN=<your GitHub token>`

## After uploading
1. Commit everything to `main`.
2. In GitHub, open **Actions** → **Refresh catalog snapshot** → **Run workflow**.
3. In GitHub repo settings, make sure Actions has **Read and write permissions**.
4. Let Render redeploy from the updated repo.
