
import os

for f in ['dashboard_link.txt', 'client_link.txt']:
    if f in os.listdir():
        os.remove(f)