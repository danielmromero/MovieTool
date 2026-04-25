Daniel's Movie Tool — deploy-ready notes

This package is prepared for hosting as a small Python web service.

Recommended host: Render

Quick deploy on Render
1. Create a new GitHub repository and upload the contents of this folder.
2. In Render, choose New > Web Service.
3. Connect the GitHub repo.
4. Render should detect Python automatically.
5. Use start command: python server.py
6. Health check path: /health
7. Deploy.

How snapshot publishing works on the free Render plan
- The site serves catalog_snapshot.json from the repo as its published catalog.
- A GitHub Actions workflow refreshes that snapshot on a schedule and commits the updated JSON back into the repo.
- Render redeploys from the updated repo, so the newest committed snapshot becomes the live catalog.
- The website no longer refreshes the whole catalog on page load.
- The in-app admin refresh button still works, but the durable source of truth is catalog_snapshot.json in GitHub.

Recommended first-run setup
1. Push this package to GitHub.
2. Open the Actions tab in GitHub and allow workflows if prompted.
3. Run the "Refresh catalog snapshot" workflow once manually so catalog_snapshot.json is populated right away.
4. After that, the workflow checks daily and refreshes whenever the snapshot is 2 or more days old.

Notes
- The workflow file is in .github/workflows/refresh_catalog.yml.
- Generated runtime files in ./data are ignored; the committed snapshot lives at the repo root.
- Manual refresh password: GetTheNewStuff
