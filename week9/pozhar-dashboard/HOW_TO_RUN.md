# How to Run the Dashboard

## Steps to start (Friday demo)

1. Open Ubuntu terminal
2. Activate virtual environment:
   source ~/myenv/bin/activate

3. Start Postgres (needed for Week 7-8 section):
   sudo service docker start
   sudo docker start pg-cdc

4. Go to the dashboard folder:
   cd ~/BuyPower-data-engineering-internship/week9/pozhar-dashboard

5. Run the dashboard:
   streamlit run dashboard.py

6. Open browser and go to:
   http://localhost:8501

7. To stop: press Ctrl+C in the terminal
