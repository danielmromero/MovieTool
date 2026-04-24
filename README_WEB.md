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

What changes were made for hosting
- The app binds to 0.0.0.0 when a PORT environment variable is present.
- The app uses the platform-provided PORT value.
- A render.yaml file is included.

Notes
- The app writes cache files into ./data. On many free hosting plans, the filesystem is ephemeral, so cache files can disappear after redeploys or restarts.
- That is okay for this app because the catalog can be refreshed live.
