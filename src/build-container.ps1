param([int] $Port = 8080, [string] $DataDirectory, [switch] $NoBuild)

$ErrorActionPreference = 'Stop'

$IsPodman = $false
if(Get-Command podman -ErrorAction Ignore){
    write-host "NOTE: Using 'podman'."
    $IsPodman = $true
    set-alias container-command podman
}elseif(Get-Command docker -ErrorAction Ignore){
    write-host "NOTE: Using 'docker'."
    set-alias container-command docker
}elseif(Get-Command container -ErrorAction Ignore){
    write-host "NOTE: Using 'container'."
    set-alias container-command container
}elseif(Get-Command wslc -ErrorAction Ignore){
    write-host "NOTE: Using 'wslc'."
    set-alias container-command wslc
}else{
    throw "You need podman, docker, WSL Containers, or a container command in order to continue."
}

if(-not $NoBuild){
    container-command build -t appviewlite .
    if($LastExitCode){ throw "container build failed with exit code $LastExitCode" }
}

if(-not $DataDirectory){
    $DataDirectory = [IO.Path]::Combine($HOME, 'AppViewLiteData')
}

$ConfigurationDirectory = [IO.Path]::Combine($DataDirectory, 'configuration')
if(-not (test-path $ConfigurationDirectory)){
    New-Item $ConfigurationDirectory -ItemType Container | out-null
}

if(-not (test-path $ConfigurationDirectory/appviewlite.env)){
    write-host "Copying appviewlite.env.example -> $ConfigurationDirectory/appviewlite.env"
    copy appviewlite.env.example "$ConfigurationDirectory/appviewlite.env"
}
if(-not (test-path $ConfigurationDirectory/appviewlite-blocklist.ini)){
    write-host "Copying appviewlite-blocklist.ini -> $ConfigurationDirectory/appviewlite-blocklist.ini"
    copy appviewlite-blocklist.ini "$ConfigurationDirectory/appviewlite-blocklist.ini"
}

container-command run -d --replace --name appviewlite -p "0.0.0.0:$($Port):8080" -v "$($DataDirectory):/data" appviewlite
if($LastExitCode){
    throw "container run returned exit code $LastExitCode"
}

$ip = '<IP ADDRESS OF YOUR LINUX VM>'
if([System.OperatingSystem]::IsLinux()){
    $ip = '127.0.0.1'
}elseif($IsPodman){
    $ip = (wsl -d podman-machine-default -- ip addr show eth0 | Select-String -Pattern 'inet\s+(\d+\.\d+\.\d+\.\d+)' | ForEach-Object { $_.Matches.Groups[1].Value }).Trim()
    if($LastExitCode){
        throw "Could not determine the IP address of the Linux VM on which Podman is running (exit code $LastExitCode)"
    }
}

write-host "Data is stored in: $DataDirectory"
write-host "To edit configuration and policies: $DataDirectory/configuration (a container restart is required after editing appviewlite.env)"
write-host "Use 'docker logs appviewlite' to see logs"
write-host "AppViewLite should now be listening on: http://$($ip):$($Port)/"

