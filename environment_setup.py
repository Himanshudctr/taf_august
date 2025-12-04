import subprocess
import sys


def set_env_var(name, value):
    """Set an environment variable permanently (user-level, no admin rights needed)."""
    try:
        subprocess.run(f'setx {name} "{value}"', shell=True, check=True)
        print(f"✅ {name} set to: {value}")
    except subprocess.CalledProcessError as e:
        print(f"⚠️ Failed to set {name}: {e}")


# === Paths (update these as per your system) ===
java_home = r"C:\Program Files\Java\jdk-11"
spark_home = r"C:\softwer Download\Softwares\spark"
hadoop_home = r"C:\softwer Download\Softwares\spark"
python_exe = 'python'  # auto-detects your current Python interpreter
import os

# === Set Environment Variables ===
set_env_var("JAVA_HOME", java_home)
set_env_var("SPARK_HOME", spark_home)
set_env_var("HADOOP_HOME", hadoop_home)
set_env_var("PYSPARK_PYTHON", python_exe)

# === Update PATH to include Java, Spark, and Hadoop bin folders ===
try:
    subprocess.run(
        f'setx PATH "%PATH%;{java_home}\\bin;{spark_home}\\bin;{hadoop_home}\\bin"',
        shell=True,
        check=False
    )
    print("✅ PATH updated successfully.")
except Exception as e:
    print(f"⚠️ PATH update failed: {e}")

# === Display summary ===
print("\n🚀 Setup complete! Please restart your terminal or IDE for changes to take effect.\n")
print("🔍 Summary of Environment Variables:")
print(f"JAVA_HOME        = {java_home}")
print(f"SPARK_HOME       = {spark_home}")
print(f"HADOOP_HOME      = {hadoop_home}")
print(f"PYSPARK_PYTHON   = {python_exe}")
print(f"Added to PATH     → {java_home}\\bin;{spark_home}\\bin;{hadoop_home}\\bin")
