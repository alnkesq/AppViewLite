$ErrorActionPreference = 'Stop'

copy .env.example .env
podman build -t appviewlite .
if($LastExitCode){ throw "podman build failed." }

wsl --distribution podman-machine-default -- mkdir -p /home/user/AppViewLiteData/configuration
if($LastExitCode){ throw "Data directory creation failed." }
copy .\appviewlite-blocklist.ini '\\wsl.localhost\podman-machine-default\home\user\AppViewLiteData\configuration'


$podmanIP = (wsl -d podman-machine-default -- ip addr show eth0 | Select-String -Pattern 'inet\s+(\d+\.\d+\.\d+\.\d+)' | ForEach-Object { $_.Matches.Groups[1].Value }).Trim()


podman run -d --replace --name appviewlite -p 0.0.0.0:8080:8080 -v /home/user/AppViewLiteData:/data --env-file .env appviewlite
if($LastExitCode){ throw "podman run failed." }


# podman logs appviewlite

write-host "AppViewLite should now be running at http://$($podmanIP):8080"

