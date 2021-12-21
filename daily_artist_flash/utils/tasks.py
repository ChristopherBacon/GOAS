import schedule
from main import main
from datetime import datetime
import os
import smtplib
import ssl
from email.message import EmailMessage



# run main function every Friday
schedule.every().monday.do(main)


def send_daly_flash_update_notification():
    msg = EmailMessage()
    update_date = datetime.today().strftime('%Y-%m-%d')
    msg.set_content(f'Daily flash has been updated {update_date}.. Check Dashboard')

    msg['Subject'] = 'Daily Flash Update'
    msg['From'] = "chris.bacon@warnerchappellpm.com"
    msg['To'] = "chris.bacon@warnerchappellpm.com"

    smtp_server = "smtp.office365.com"
    port = 587  # For starttls
    server_email = os.getenv('NOTIFICATIONS_EMAIL_UN')
    server_password = os.getenv('NOTIFICATIONS_EMAIL_PW')

    # Create a secure SSL context
    context = ssl.create_default_context()

    # Try to log in to server and send email
    try:
        server = smtplib.SMTP(smtp_server, port)
        server.starttls(context=context)  # Secure the connection
        server.login(server_email, server_password)
        server.send_message(msg)
    except Exception as e:
        # Print any error messages to stdout
        print(e)
    finally:
        server.quit()

    return


while True:
    # should now run every Friday
    schedule.run_pending()
    send_daly_flash_update_notification()

