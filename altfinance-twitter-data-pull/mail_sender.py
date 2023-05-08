from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import os 
from dotenv import load_dotenv
import io
from email.mime.application import MIMEApplication
import smtplib

load_dotenv()


def send_email(send_to, subject,body):
    send_from = os.environ['OUTLOOK_SENDER']
    password = os.environ['OUTLOOK_PASSWORD']
    multipart = MIMEMultipart()
    multipart["From"] = send_from
    multipart["To"] = send_to
    multipart["Subject"] = subject
    multipart.attach(MIMEText(body, "html"))
    server = smtplib.SMTP("smtp-mail.outlook.com", 587)
    server.starttls()
    server.login(multipart["From"], password)
    server.sendmail(multipart["From"], multipart["To"], multipart.as_string())
    server.quit()