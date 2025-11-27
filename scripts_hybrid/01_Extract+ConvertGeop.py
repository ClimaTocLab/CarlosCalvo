import subprocess

comando = "./01_Extract_zlnsp.sh"
subprocess.run(comando, shell=True, check=True, capture_output=True, text=True)

comando = "./01_compute_geop.sh"
subprocess.run(comando, shell=True, check=True, capture_output=True, text=True)

comando = "./01_compute_pres.sh"
subprocess.run(comando, shell=True, check=True, capture_output=True, text=True)
