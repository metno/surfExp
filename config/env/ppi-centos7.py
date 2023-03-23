"""Environment specific file in python syntax."""
import os
import sys

# Ecflow server uses SSL
os.environ["ECF_SSL"] = "1"
# Not all dependencies for pysurfex in python version
sys.path.insert(
    0, "/modules/centos7/user-apps/suv/pysurfex/addons/lib/python3.7/site-packages/"
)
