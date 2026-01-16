# COMPLETE STEP-BY-STEP RAILWAY DEPLOYMENT GUIDE

Follow these steps EXACTLY and you'll have your Python API running in the cloud in 10 minutes!

---

## PART 1: Upload Files to GitHub (5 minutes)

### Step 1: Go to GitHub
1. Open your browser
2. Go to https://github.com
3. Sign in (or create a free account if you don't have one)

### Step 2: Create New Repository
1. Click the **"+"** button in the top-right corner
2. Click **"New repository"**
3. Fill in:
   - **Repository name**: `restaurant-enrichment-api`
   - **Description**: "Python API for enriching restaurant data"
   - **Public** (select this - it's free)
   - ✅ Check "Add a README file"
4. Click **"Create repository"**

### Step 3: Upload Your Files
1. You should now be on your new repository page
2. Click **"Add file"** → **"Upload files"**
3. Download the `railway-deployment` folder I created
4. Drag and drop these 4 files into the upload area:
   - `api.py`
   - `secondary_enrichment.py`
   - `requirements.txt`
   - `Procfile`
5. Scroll down, add commit message: "Initial commit"
6. Click **"Commit changes"**

✅ **Your GitHub repo is ready!**

---

## PART 2: Deploy to Railway (5 minutes)

### Step 4: Sign Up for Railway
1. Go to https://railway.app
2. Click **"Login"**
3. Click **"Login with GitHub"**
4. Authorize Railway to access your GitHub account
5. You'll be taken to your Railway dashboard

### Step 5: Create New Project
1. Click **"New Project"** (big purple button)
2. Select **"Deploy from GitHub repo"**
3. You'll see a list of your repositories
4. Find and click **"restaurant-enrichment-api"**
5. Railway will say "Deploy now" - Click it!

### Step 6: Wait for Deployment
1. Railway will now:
   - ✓ Detect it's a Python app
   - ✓ Install dependencies from requirements.txt
   - ✓ Start your server with gunicorn
2. This takes about 2-3 minutes
3. You'll see logs scrolling - wait until you see "Build successful" ✅

### Step 7: Get Your API URL
1. Click on your deployment (in the Railway dashboard)
2. Go to the **"Settings"** tab
3. Scroll down to **"Domains"**
4. Click **"Generate Domain"**
5. Railway will give you a URL like: `https://restaurant-enrichment-api-production-xxxx.up.railway.app`
6. **COPY THIS URL** - you'll need it in the next step! 📋

### Step 8: Test Your API
1. Open a new browser tab
2. Go to your Railway URL (the one you just copied)
3. You should see:
   ```json
   {
     "message": "Restaurant Enrichment API is running!",
     "status": "ok"
   }
   ```
4. ✅ **Your API is live!**

---

## PART 3: Update Your React App in Gemini Studio (3 minutes)

### Step 9: Update API URL in React App

Now you need to tell your Gemini Studio app to use the Railway URL instead of localhost.

**Send this prompt to Gemini Studio:**

```
Please update the Python server connection to use my Railway deployment instead of localhost.

My Railway URL is: [PASTE YOUR URL HERE]

Make these changes:

1. In lib/pythonServerManager.ts:
   - Add a constant at the top: const API_URL = 'https://your-railway-url.railway.app';
   - Update checkServerHealth() to use: `${API_URL}/health`
   - Update all fetch calls to use API_URL instead of http://localhost:5000

2. In components/SecondaryEnrichmentSection.tsx:
   - Update the fetch call in handleRunEnrichment to use: `${API_URL}/enrich`
   - Remove the terminal instructions UI since server is now in the cloud
   - Change "Start Enrichment Server" button text to "Test Connection"
   - When user clicks it, just check if API is reachable (no need to start anything)

3. Update the UI flow:
   - Remove "waiting for server" state
   - Remove terminal command display
   - Just show: Checking connection → Ready → Process

Please implement these changes.
```

**Replace `[PASTE YOUR URL HERE]` with your actual Railway URL!**

### Step 10: Test in Your App
1. Once Gemini updates your code
2. Go to Secondary Scrape tab in your app
3. Click "Test Connection" (or whatever button Gemini created)
4. It should show "Connected!" or "Server Ready"
5. Click "Load & Process Advanced Scraping"
6. It works! 🎉

---

## TROUBLESHOOTING

### Problem: Railway deployment failed
**Solution**: Check the logs in Railway dashboard
- Common issue: Missing dependencies → Add them to requirements.txt
- Click "Redeploy" button to try again

### Problem: CORS error in browser console
**Solution**: The API already has CORS enabled for all origins
- Make sure you're using the full URL (including https://)
- Check browser console for exact error

### Problem: API returns 500 error
**Solution**: Check Railway logs
1. Go to Railway dashboard
2. Click on your deployment
3. Click "View Logs"
4. See what error the Python server is throwing

### Problem: "Cannot connect to API"
**Solution**: 
- Make sure you copied the FULL Railway URL (including https://)
- Test the URL in your browser first - should show the welcome message
- Check that Railway app is still running (go to dashboard)

---

## WHAT YOU JUST DID

✅ Created a GitHub repository with your Python code
✅ Deployed it to Railway (free cloud hosting)
✅ Got a public URL that's always accessible
✅ Updated your React app to use the cloud API
✅ No more localhost! Everything runs in the cloud!

---

## FREE TIER LIMITS (Railway)

- **500 hours per month** of runtime (about 16 hours per day)
- **100GB bandwidth** per month
- More than enough for your use case!
- If you exceed limits, Railway will notify you

---

## FUTURE UPDATES

When you want to update your Python code:

1. Make changes to files in GitHub repo
2. Commit changes
3. Railway automatically redeploys! (takes 2 minutes)

---

## Need Help?

If anything doesn't work:
1. Check Railway logs
2. Check browser console for errors  
3. Make sure your Railway URL is correct
4. Test the /health endpoint in your browser

Your API is now in the cloud and accessible from anywhere! 🚀
