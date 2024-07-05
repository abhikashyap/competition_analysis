FROM python:3.10
WORKDIR /app
COPY . /app
RUN pip install -r requirements.txt

# RUN apt-get update && apt-get install -y wget unzip && \
#     wget https://dl.google.com/linux/direct/google-chrome-stable_current_amd64.deb && \
#     apt install -y ./google-chrome-stable_current_amd64.deb && \
#     rm google-chrome-stable_current_amd64.deb && \
#     apt-get clean 
    # && prefect cloud login --key pnu_vTVrNhdHEyxnfayjPLPSLclziDZImW2kA97W --workspace blooprint/default
# RUN ["prefect" "cloud" "login" "-k" "pnu_vTVrNhdHEyxnfayjPLPSLclziDZImW2kA97W"]
CMD [ "streamlit", "run", "test2.py"]
