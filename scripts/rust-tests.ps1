param(
    [ValidateSet("fast", "db", "api", "cli", "full", "snapshot")]
    [string]$Suite = "fast",

    [ValidateRange(1, 256)]
    [int]$Jobs = 4,

    [ValidateRange(1, 256)]
    [int]$TestThreads = 4
)

$ErrorActionPreference = "Stop"

$arguments = switch ($Suite) {
    "fast" { @("test", "--workspace") }
    "db" { @("test", "--workspace", "--features", "db-tests") }
    "api" { @("test", "-p", "top_contract_analysis_rs", "--features", "api-tests") }
    "cli" { @("test", "-p", "top_contract_analysis_rs", "--features", "cli-tests") }
    "full" { @("test", "--workspace", "--features", "expensive-tests") }
    "snapshot" { @("test", "-p", "top_contract_analysis_rs", "--features", "db-tests export-snapshot") }
}

$arguments += @("-j", $Jobs, "--", "--test-threads", $TestThreads)

Write-Host "cargo $($arguments -join ' ')"
& cargo @arguments
exit $LASTEXITCODE
