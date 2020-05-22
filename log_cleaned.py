import subprocess

with open("log.txt", "w+") as output:
    subprocess.call(["python", "/home/vca_rishik/rishik/COVID-19-tweets/clean_data.py"], stdout=output);