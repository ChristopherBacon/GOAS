import schedule
from daily_artist_flash.main import main
import time

# run main function every Monday
schedule.every().monday.do(main)

while True:
    schedule.run_pending()
    # sleep 24 hours
    time.sleep(86400)