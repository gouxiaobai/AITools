param(
    [string]$Token,
    [switch]$NoTest
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Fail {
    param(
        [string]$Message,
        [int]$Code = 1
    )
    Write-Host "[ERROR] $Message" -ForegroundColor Red
    exit $Code
}

function Info {
    param([string]$Message)
    Write-Host "[INFO]  $Message" -ForegroundColor Cyan
}

function Ok {
    param([string]$Message)
    Write-Host "[OK]    $Message" -ForegroundColor Green
}

if (-not $Token) {
    $Token = Read-Host "Enter Notion Internal Integration Token (ntn_...)"
}

$Token = $Token.Trim()
if ($Token.StartsWith("'") -and $Token.EndsWith("'")) {
    $Token = $Token.Trim("'")
}
if ($Token.StartsWith('"') -and $Token.EndsWith('"')) {
    $Token = $Token.Trim('"')
}

if ([string]::IsNullOrWhiteSpace($Token)) {
    Fail -Message "Token is empty." -Code 10
}
if ($Token -match "\s") {
    Fail -Message "Token contains whitespace. Paste token without spaces/newlines." -Code 11
}
if (-not $Token.StartsWith("ntn_")) {
    Fail -Message "Token format looks invalid: must start with ntn_." -Code 12
}

[System.Environment]::SetEnvironmentVariable("NOTION_TOKEN", $Token, "User")
$env:NOTION_TOKEN = [System.Environment]::GetEnvironmentVariable("NOTION_TOKEN", "User")

if (-not $env:NOTION_TOKEN) {
    Fail -Message "Failed to set NOTION_TOKEN in user environment." -Code 13
}

$preview = if ($env:NOTION_TOKEN.Length -ge 8) {
    $env:NOTION_TOKEN.Substring(0, 8)
} else {
    $env:NOTION_TOKEN
}
Ok -Message ("NOTION_TOKEN saved (preview: {0}..., len={1})" -f $preview, $env:NOTION_TOKEN.Length)

if ($NoTest) {
    Info -Message "Skipped API connectivity test due to -NoTest."
    exit 0
}

Info -Message "Testing Notion API connectivity: GET /v1/users/me"
$headers = @{
    Authorization   = "Bearer $env:NOTION_TOKEN"
    "Notion-Version" = "2022-06-28"
}

try {
    $resp = Invoke-RestMethod -Method Get -Uri "https://api.notion.com/v1/users/me" -Headers $headers
    $objectType = $null
    $userType = $null
    $userId = $null
    $userName = $null
    $botId = $null
    $botName = $null

    if ($resp) {
        if ($resp.PSObject.Properties.Name -contains "object") { $objectType = $resp.object }
        if ($resp.PSObject.Properties.Name -contains "type") { $userType = $resp.type }
        if ($resp.PSObject.Properties.Name -contains "id") { $userId = $resp.id }
        if ($resp.PSObject.Properties.Name -contains "name") { $userName = $resp.name }

        if (($resp.PSObject.Properties.Name -contains "bot") -and $resp.bot) {
            if ($resp.bot.PSObject.Properties.Name -contains "id") { $botId = $resp.bot.id }
            if ($resp.bot.PSObject.Properties.Name -contains "name") { $botName = $resp.bot.name }
        }
    }

    Ok -Message "Connectivity test passed."
    if ($objectType) { Info -Message "object=$objectType" }
    if ($userType) { Info -Message "type=$userType" }
    if ($userId) { Info -Message "id=$userId" }
    if ($userName) { Info -Message "name=$userName" }
    if ($botId) { Info -Message "bot_id=$botId" }
    if ($botName) { Info -Message "bot_name=$botName" }
    exit 0
}
catch {
    $err = $_
    $ex = $err.Exception
    $statusCode = $null
    $body = $null

    $hasResponseProp = $ex -and ($ex.PSObject.Properties.Name -contains "Response")
    if ($hasResponseProp -and $null -ne $ex.Response) {
        try { $statusCode = $ex.Response.StatusCode.value__ } catch {}
    }
    $hasErrorDetailsProp = $err.PSObject.Properties.Name -contains "ErrorDetails"
    if ($hasErrorDetailsProp -and $null -ne $err.ErrorDetails -and $err.ErrorDetails.Message) {
        $body = $err.ErrorDetails.Message
    } else {
        $body = if ($ex) { $ex.Message } else { $err.ToString() }
    }

    Write-Host "[ERROR] Connectivity test failed." -ForegroundColor Red
    if ($statusCode) { Write-Host "[ERROR] HTTP status: $statusCode" -ForegroundColor Red }
    if ($body) { Write-Host "[ERROR] Detail: $body" -ForegroundColor Red }
    exit 20
}
