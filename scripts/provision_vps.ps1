# 建立新的 Gravia VPS（AWS EC2）並部署模擬盤。
# 需求：本機已完成 AWS 認證（aws login 或 aws configure）。
# 用法：powershell -ExecutionPolicy Bypass -File scripts\provision_vps.ps1
#       可加 -Region eu-west-1 -InstanceType t3.large -VolumeGB 40

param(
  [string]$Region = "eu-west-1",
  [string]$InstanceType = "t3.large",
  [int]$VolumeGB = 40,
  [string]$KeyName = "gravia-key",
  [string]$SgName = "gravia-sg",
  [string]$TagName = "gravia-sim"
)

$ErrorActionPreference = "Stop"
$aws = "C:\Program Files\Amazon\AWSCLIV2\aws.exe"
$proj = Split-Path -Parent $PSScriptRoot
$keyPath = Join-Path $env:USERPROFILE "Downloads\$KeyName.pem"

function AWS { & $aws --region $Region @args }

Write-Host "== 1/6 確認認證 =="
$who = AWS sts get-caller-identity --output json | ConvertFrom-Json
Write-Host ("  帳號 {0}" -f $who.Account)

Write-Host "== 2/6 金鑰對 =="
$existing = AWS ec2 describe-key-pairs --key-names $KeyName --output json 2>$null
if ($LASTEXITCODE -ne 0) {
  $material = AWS ec2 create-key-pair --key-name $KeyName --query KeyMaterial --output text
  [System.IO.File]::WriteAllText($keyPath, $material)
  icacls $keyPath /inheritance:r /grant:r "$($env:USERNAME):(R)" | Out-Null
  Write-Host "  已建立新金鑰：$keyPath"
} else {
  if (-not (Test-Path $keyPath)) { throw "AWS 上已有金鑰 $KeyName，但本機找不到 $keyPath；請改用 -KeyName 換個名字" }
  Write-Host "  沿用既有金鑰：$keyPath"
}

Write-Host "== 3/6 安全群組（只開 SSH 給你目前的 IP）=="
$myIp = (Invoke-RestMethod -Uri "https://checkip.amazonaws.com").Trim()
$sgId = AWS ec2 describe-security-groups --filters "Name=group-name,Values=$SgName" --query "SecurityGroups[0].GroupId" --output text 2>$null
if (-not $sgId -or $sgId -eq "None") {
  $sgId = AWS ec2 create-security-group --group-name $SgName --description "Gravia sim: SSH only" --query GroupId --output text
}
AWS ec2 authorize-security-group-ingress --group-id $sgId --protocol tcp --port 22 --cidr "$myIp/32" 2>$null | Out-Null
Write-Host "  $sgId（允許 $myIp/32 的 22 埠）"

Write-Host "== 4/6 取得 Ubuntu 24.04 AMI =="
$ami = AWS ssm get-parameters --names "/aws/service/canonical/ubuntu/server/24.04/stable/current/amd64/hvm/ebs-gp3/ami-id" --query "Parameters[0].Value" --output text
Write-Host "  $ami"

Write-Host "== 5/6 啟動執行個體 $InstanceType =="
$userData = @"
#!/bin/bash
set -e
apt-get update -y
apt-get install -y python3-venv python3-pip git sqlite3
sudo -u ubuntu python3 -m venv /home/ubuntu/venv
sudo -u ubuntu /home/ubuntu/venv/bin/pip install --upgrade pip
mkdir -p /home/ubuntu/gravia_live && chown ubuntu:ubuntu /home/ubuntu/gravia_live
timedatectl set-timezone UTC
"@
$b64 = [Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes($userData))
$bdm = "[{`"DeviceName`":`"/dev/sda1`",`"Ebs`":{`"VolumeSize`":$VolumeGB,`"VolumeType`":`"gp3`",`"DeleteOnTermination`":true}}]"
$id = AWS ec2 run-instances --image-id $ami --instance-type $InstanceType --key-name $KeyName `
      --security-group-ids $sgId --block-device-mappings $bdm --user-data $b64 `
      --instance-initiated-shutdown-behavior stop `
      --tag-specifications "ResourceType=instance,Tags=[{Key=Name,Value=$TagName}]" `
      --query "Instances[0].InstanceId" --output text
Write-Host "  $id 啟動中..."
AWS ec2 wait instance-running --instance-ids $id
$ip = AWS ec2 describe-instances --instance-ids $id --query "Reservations[0].Instances[0].PublicIpAddress" --output text

Write-Host "== 6/6 完成 =="
Write-Host "  執行個體：$id"
Write-Host "  公網 IP ：$ip"
Write-Host "  金鑰    ：$keyPath"
Write-Host ""
Write-Host "  連線： ssh -i `"$keyPath`" ubuntu@$ip"
Write-Host "  （開機腳本安裝 python/venv 約需 2 分鐘，之後再部署專案）"
@{ instanceId = $id; publicIp = $ip; region = $Region; keyPath = $keyPath; sg = $sgId } |
  ConvertTo-Json | Set-Content (Join-Path $proj "vps_info.json") -Encoding UTF8
Write-Host "  資訊已寫入 vps_info.json"
