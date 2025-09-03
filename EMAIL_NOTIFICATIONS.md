# Email Notifications for DNO Generator

## Overview

The DNO generator now sends email notifications after each run:
- **Success emails** with complete statistics and results
- **Failure emails** with error details and troubleshooting steps

## Configuration

### Required Setup

1. **Install SendGrid** (already done in venv):
   ```bash
   source .venv/bin/activate
   pip install sendgrid
   ```

2. **Configure SendGrid API Key** in `.env`:
   ```bash
   SENDGRID_API_KEY=your-sendgrid-api-key-here
   ```

   ⚠️ **Current Status**: The API key is returning 401 Unauthorized. You'll need to:
   - Verify the API key is correct
   - Ensure the API key has proper permissions
   - Check if the key is active in SendGrid dashboard

### Optional Configuration

Add these to `.env` to customize:

```bash
# Sender email (must be verified in SendGrid)
SENDGRID_FROM_EMAIL=dno-generator@teliax.com

# Recipient email
SENDGRID_TO_EMAIL=engineering@teliax.com

# Disable emails if needed
DNO_EMAIL_NOTIFICATIONS=false
```

## Email Content

### Success Email
- Runtime and performance metrics
- Total NPAs processed
- Number of assigned/unassigned combinations
- ITG traceback records count
- List of generated files

### Failure Email
- Which NPA failed
- Error message details
- Progress before failure
- Troubleshooting steps

## How It Works

1. **Automatic**: Emails are sent automatically after each run
2. **Graceful Degradation**: If SendGrid is not configured or fails, the script continues normally
3. **No Dependencies**: Script works without SendGrid installed (just skips emails)

## Testing Email Configuration

Run this test script to verify email setup:

```python
from dno_gen import send_email_notification

# Test basic email
success = send_email_notification(
    subject="DNO Generator - Test Email",
    html_content="<h2>Test Successful</h2><p>Email notifications are working!</p>"
)

if success:
    print("✅ Email sent successfully!")
else:
    print("❌ Email failed - check configuration")
```

## Troubleshooting

### 401 Unauthorized Error
- Verify API key is correct in `.env`
- Check API key permissions in SendGrid dashboard
- Ensure API key is not expired or revoked

### 403 Forbidden Error
- Verify sender email is verified in SendGrid
- Check domain authentication settings

### Emails Not Sending
1. Check if SendGrid is installed: `pip list | grep sendgrid`
2. Verify environment variables are loaded
3. Check `DNO_EMAIL_NOTIFICATIONS` is not set to `false`
4. Enable debug mode: `export DNO_DEBUG=true`

## SendGrid API Key Setup

To get a SendGrid API key:

1. Log in to [SendGrid Dashboard](https://app.sendgrid.com/)
2. Go to Settings → API Keys
3. Click "Create API Key"
4. Choose "Full Access" or "Restricted Access" with:
   - Mail Send - Full Access
5. Copy the key (it won't be shown again!)
6. Add to `.env` file

## Security Notes

- Never commit the `.env` file with API keys
- Use environment-specific keys (dev/prod)
- Rotate keys periodically
- Consider using restricted access keys with minimal permissions
