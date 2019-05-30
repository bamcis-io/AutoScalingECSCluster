Import-Module AWSPowerShell

Set-AWSCredential -ProfileName "mhaken-dev"

$Key = "AutoScalingECSCluster.template"
$Bucket = "mhaken-lambda"
$Obj = "AutoScalingECSCluster"
$Path = "$([System.IO.Path]::GetDirectoryName($MyInvocation.InvocationName))\$Key"

& aws cloudformation package --template-file $Path --s3-bucket $Bucket --output-template-file 

$Content = Get-Content -Path  -Raw
$Token = [System.Guid]::NewGuid()
$Name =  "change-set-$((Get-Date).ToString("yyyy-MM-ddThh-mm-ssZ"))"

$Splat = @{}

try
{
    $Stack = Get-CFNStack -StackName $Obj -ErrorAction SilentlyContinue
}
catch
{
    $Splat.Add("ChangeSetType", [Amazon.CloudFormation.ChangeSetType]::CREATE)
}

New-CFNChangeSet -StackName $Obj -ChangeSetName $Name @Splat -Capability CAPABILITY_IAM `
	    -Parameter @(
        @{ParameterKey = "ASGMaxSize"; ParameterValue = "5"},
        @{ParameterKey = "ASGMinSize"; ParameterValue = "1" },
        @{ParameterKey = "Cooldown"; ParameterValue = "200"},     
        @{ParameterKey = "HealthCheckGracePeriod"; ParameterValue = "180"},
        @{ParameterKey = "HighThreshold"; ParameterValue = "3"},
        @{ParameterKey = "InstanceType"; ParameterValue = "m5.large"},
        @{ParameterKey = "KeyName"; ParameterValue = "mhaken-dev-us-east-1"},
        @{ParameterKey = "LowThreshold"; ParameterValue = "1"},
        @{ParameterKey = "MaxContainerCPUUnits"; ParameterValue = "1024"},
        @{ParameterKey = "MaxContainerMemory"; ParameterValue = "2048"},
		@{ParameterKey = "NotificationEmail"; ParameterValue = "mhaken@amazon.com"},
        @{ParameterKey = "Subnets"; ParameterValue = "subnet-94f13ef3,subnet-3f458311,subnet-9cd952d6"},
        @{ParameterKey = "UseSpot"; ParameterValue = "false"}
) `
        -TemplateBody $Content

while ((Get-CFNChangeSet -ChangeSetName $Name -StackName $Obj).Status -ne [Amazon.CloudFormation.ChangeSetStatus]::CREATE_COMPLETE)
{
    Start-Sleep -Seconds 5
}

Start-CFNChangeSet -StackName $Obj -ChangeSetName $Name -ClientRequestToken $Token
