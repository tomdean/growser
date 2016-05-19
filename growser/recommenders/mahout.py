import os
import subprocess


def run_recommendations(model_id: int, ratings: str, output: str):
    """Minimal wrapper to exec Mahout Java app."""
    cmd = ["mvn", "exec:java", "-DbatchSize=100",
           "-DmodelID={}".format(model_id),
           "-Dsrc=" + os.path.abspath(ratings),
           "-Dout=" + os.path.abspath(output)]
    subprocess.call(cmd, cwd="../growser-mahout/")
