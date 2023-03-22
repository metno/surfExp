import sys
import os

# Ecflow server uses SSL
os.environ["ECF_SSL"] = "1"
# Not all dependencies for pysurfex in python version
# sys.path.insert(0, "/modules/bionic/user-apps/suv/pysurfex/addons/lib/python3.8/site-packages/")
sys.path.insert(
    0, "/modules/centos7/user-apps/suv/pysurfex/addons/lib/python3.7/site-packages/"
)
