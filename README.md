# Auto Scaling ECS Cluster

This serverless project deploys an Auto Scaling ECS cluster utilizing custom computed metrics for the number of schedulable
containers. It also manages scale-in actions by draining tasks on the terminating node before finishing the lifecycle action. The metric publishing, alarm thresholds, task draining, and lifecycle hook completions are all managed by Lambda functions. 

## Table of Contents
- [Usage](#usage)
- [Revision History](#revision-history)

## Usage

While you could potentially use this in production, you would likely want to use this as a reference architecture/implementation. Building in appropriate error checking, metric collection, self-healing, etc is critical for production workloads. A number of checks and CloudWatch alarms for the Lambda functions are built-in to the CloudFormation template.

There is a recurring Lambda function that computes the appropriate scale-in threshold and periodically updates the CloudWatch alarm that triggers scale-in for the Auto Scaling Group. The threshold is initially published as the same as the scale-out alarm, but will be updated shortly after deployment by the Lambda function. It ensures that the scale-in threshold accounts for leaving 1 entire task node empty plus the scale-out buffer available so that a scale-in action doesn't just cause an immediate scale-out (which would happen if the scale-in threshold was only set to the number of schedulable containers for 1 task node).

No unit tests exist yet for the Lambda functions, they're TODO.

## Revision History

### 1.0.0
Initial release of the application.
