# Add util directory to python path
import os
import sys

DIR_PATH = os.path.dirname(os.path.realpath(__file__))
SOURCE_PATH = os.path.join(DIR_PATH, "..", "..", "..", "layers", "util")
sys.path.append(SOURCE_PATH)


# Setting up environment variable
os.environ["EMAIL_NOTIFY"] = "yes"
os.environ["MANAGER_EMAIL"] = "william.intan@unimelb.edu.au"
os.environ["SENDER_EMAIL"] = "services@umccr.org"
os.environ["SLACK_CHANNEL"] = "#agha-gdr"
os.environ["SLACK_HOST"] = "hooks.slack.com"
os.environ["SLACK_NOTIFY"] = "yes"
