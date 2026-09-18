# NPSGW 部署脚本：上传新包和配置到 124 服务器并重启服务
# 用法：在项目目录下执行 .\deploy.ps1
# 如 SSH 用户名/端口不是默认，修改下面 $server

$server = "root@192.168.1.124"
$remote = "/opt/pcs/nps"

Write-Host "==> 上传 npsgw-linux-arm64 和 config.yaml ..."
scp npsgw-linux-arm64 config.yaml "${server}:${remote}/"
if ($LASTEXITCODE -ne 0) { Write-Host "上传失败"; exit 1 }

Write-Host "==> 重启服务 ..."
ssh $server "cd $remote; systemctl restart nps; sleep 2; journalctl -u nps -n 30 --no-pager"

Write-Host "==> 部署完成。实时日志: ssh $server 'journalctl -u nps -f'"
