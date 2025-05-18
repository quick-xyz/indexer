import os
import sys

os.chdir(os.path.dirname(__file__))
curr_wd = os.getcwd()

# # Add Flask app & parent directory to Python PATH
sys.path.insert(0, curr_wd)
print(sys.path)

import wesmol_api

application = wesmol_api.app
