using Amazon.AutoScaling;
using Amazon.AutoScaling.Model;
using Amazon.CloudWatch;
using Amazon.CloudWatch.Model;
using Amazon.ECS;
using Amazon.ECS.Model;
using Amazon.Lambda.CloudWatchEvents;
using Amazon.Lambda.CloudWatchEvents.ECSEvents;
using Amazon.Lambda.CloudWatchEvents.ScheduledEvents;
using Amazon.Lambda.Core;
using Amazon.SimpleNotificationService;
using Amazon.SimpleNotificationService.Model;
using BAMCIS.AWSLambda.Common;
using BAMCIS.AWSLambda.Common.Events.AutoScaling;
using BAMCIS.ChunkExtensionMethod;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Reflection;
using System.Text;

// Assembly attribute to enable the Lambda function's JSON input to be converted into a .NET class.
[assembly: LambdaSerializer(typeof(Amazon.Lambda.Serialization.Json.JsonSerializer))]

namespace BAMCIS.LambdaFunctions.AutoScalingECSCluster
{
    public class Entrypoint
    {
        #region Private Fields

        private static IAmazonCloudWatch cloudWatchClient;
        private static IAmazonAutoScaling autoScalingClient;
        private static IAmazonSimpleNotificationService snsClient;
        private static IAmazonECS ecsClient;
        private static string snsTopic;
        private static string ecsCluster;
        private static int maxCPUUnits;
        private static int maxMemory;
        private static readonly string emailSubject = "Auto Scaling ECS Lambda Function Failure";

        #endregion

        #region Constructors

        /// <summary>
        /// Default constructor that Lambda will invoke.
        /// </summary>
        public Entrypoint()
        {
        }

        static Entrypoint()
        {
            cloudWatchClient = new AmazonCloudWatchClient();
            autoScalingClient = new AmazonAutoScalingClient();
            snsClient = new AmazonSimpleNotificationServiceClient();
            ecsClient = new AmazonECSClient();
            ecsCluster = Environment.GetEnvironmentVariable("ECS_CLUSTER");
            snsTopic = Environment.GetEnvironmentVariable("SNS_TOPIC");
            Int32.TryParse(Environment.GetEnvironmentVariable("MAX_CPU"), out maxCPUUnits);
            Int32.TryParse(Environment.GetEnvironmentVariable("MAX_MEMORY"), out maxMemory);
        }

        #endregion

        #region Public Methods

        /// <summary>
        /// This entrypoint is triggered when a container instance task changes state 
        /// to STOPPED. This function will check to see if there any more remaining
        /// tasks, and if not, complete the lifecycle hook.
        /// </summary>
        /// <param name="cwEvent"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public async System.Threading.Tasks.Task CompleteLifecycleHookAsync(ECSTaskStateChangeEvent cwEvent, ILambdaContext context)
        {
            context.LogInfo($"Received CloudWatch Event for an ECS task state change:\n${JsonConvert.SerializeObject(cwEvent)}");

            // Let this throw since it logs all errors already
            List<string> arns = await CheckForTasksOnContainerInstanceAsync(cwEvent.Detail.ClusterArn, cwEvent.Detail.ContainerInstanceArn, context);

            if (arns.Any())
            {
                context.LogInfo($"Waiting on ${arns.Count} to finish draining.");
                return;
            }

            context.LogInfo("No tasks remaining on container instance, completing lifecycle hook.");

            // First get the container instance details where the task completed
            DescribeContainerInstancesRequest describeRequest = new DescribeContainerInstancesRequest()
            {
                Cluster = cwEvent.Detail.ClusterArn,
                ContainerInstances = new List<string>() { cwEvent.Detail.ContainerInstanceArn }
            };

            DescribeContainerInstancesResponse describeResponse;

            try
            {
                describeResponse = await ecsClient.DescribeContainerInstancesAsync(describeRequest);
            }
            catch (Exception e)
            {
                string message = "Failed successfully complete a DescribeContainerInstances request.";
                context.LogError(message, e);
                await SNSNotify($"{message}\n{e.GetType().FullName} : {e.Message}", context);
                throw e;
            }

            if (describeResponse.HttpStatusCode != HttpStatusCode.OK)
            {
                string message = $"Failed to describe container instance {cwEvent.Detail.ContainerInstanceArn} with status code ${(int)describeResponse.HttpStatusCode} ${describeResponse.HttpStatusCode}. Request Id : ${describeResponse.ResponseMetadata.RequestId}";
                context.LogError(message);
                await SNSNotify(message, context);
                throw new Exception(message);
            }

            string autoScalingGroupName = Environment.GetEnvironmentVariable("ASG_NAME");
            string lifecycleHookName = Environment.GetEnvironmentVariable("LIFECYCLE_HOOK_NAME");

            if (String.IsNullOrEmpty(autoScalingGroupName))
            {
                string message = "The ASG_NAME environment variable is not set or was empty and is required.";
                context.LogError(message);
                await SNSNotify(message, context);
                throw new Exception(message);
            }

            if (String.IsNullOrEmpty(lifecycleHookName))
            {
                string message = "The LIFECYCLE_HOOK_NAME environment variable is not set or was empty and is required.";
                context.LogError(message);
                await SNSNotify(message, context);
                throw new Exception(message);
            }

            CompleteLifecycleActionRequest lifecycleRequest = new CompleteLifecycleActionRequest()
            {
                AutoScalingGroupName = autoScalingGroupName,
                InstanceId = describeResponse.ContainerInstances.First().Ec2InstanceId,
                LifecycleActionResult = "CONTINUE",
                LifecycleHookName = lifecycleHookName
            };

            // Don't wrap this in try/catch, it already logs and notifies
            CompleteLifecycleActionResponse lifecycleResponse = await CompleteLifecycleHookAsync(lifecycleRequest, context);

            context.LogInfo($"Function complete for instance ${lifecycleRequest.InstanceId} in ASG ${lifecycleRequest.AutoScalingGroupName} for hook ${lifecycleRequest.LifecycleHookName}.");
        }

        /// <summary>
        /// This entrypoint is triggered when an Auto Scaling lifecycle hook
        /// CloudWatch event is triggered to terminate an instance. The function
        /// will set the container instance to draining.
        /// </summary>
        /// <param name="cwEvent"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public async System.Threading.Tasks.Task DrainContainersAsync(CloudWatchEvent<AutoScalingLifecycleEvent> cwEvent, ILambdaContext context)
        {
            context.LogInfo($"Received CloudWatch Event for an Auto Scaling Lifecycle Hook:\n${JsonConvert.SerializeObject(cwEvent)}");

            ListContainerInstancesResponse listResponse;

            try
            {
                ListContainerInstancesRequest request = new ListContainerInstancesRequest()
                {
                    Cluster = ecsCluster,
                    Filter = $"ec2InstanceId == {cwEvent.Detail.EC2InstanceId}"
                };

                listResponse = await ecsClient.ListContainerInstancesAsync(request);
            }
            catch (Exception e)
            {
                string message = "Failed to list container instances.";
                context.LogError(message, e);
                await SNSNotify($"{message}\n{e.GetType().FullName} : {e.Message}", context);
                throw e;
            }

            if (listResponse.HttpStatusCode != HttpStatusCode.OK)
            {
                string message = $"Failed to list container instances with status code ${(int)listResponse.HttpStatusCode} ${listResponse.HttpStatusCode}. Request Id : ${listResponse.ResponseMetadata.RequestId}";
                context.LogError(message);
                await SNSNotify(message, context);
                throw new Exception(message);
            }

            if (!listResponse.ContainerInstanceArns.Any())
            {
                string message = $"No container instances found in cluster {ecsCluster} matching EC2InstanceId {cwEvent.Detail.EC2InstanceId}.";
                context.LogError(message);
                await SNSNotify(message, context);
                throw new Exception(message);
            }

            if (listResponse.ContainerInstanceArns.Count > 1)
            {
                string message = $"More than 1 container instance found in cluster {ecsCluster} matching EC2InstanceId {cwEvent.Detail.EC2InstanceId}.\n{String.Join(", ", listResponse.ContainerInstanceArns)}";
                context.LogError(message);
                await SNSNotify(message, context);
                throw new Exception(message);
            }

            UpdateContainerInstancesStateResponse response;

            try
            {
                UpdateContainerInstancesStateRequest request = new UpdateContainerInstancesStateRequest()
                {
                    Cluster = ecsCluster,
                    ContainerInstances = listResponse.ContainerInstanceArns,
                    Status = ContainerInstanceStatus.DRAINING
                };

                response = await ecsClient.UpdateContainerInstancesStateAsync(request);
            }
            catch (Exception e)
            {
                string message = "Failed to update the container instance state.";
                context.LogError(message, e);
                await SNSNotify($"{message}\n{e.GetType().FullName} : {e.Message}", context);
                throw e;
            }

            if (response.HttpStatusCode != HttpStatusCode.OK)
            {
                string message = $"Failed to set ECS container instance state with status code ${(int)response.HttpStatusCode} ${response.HttpStatusCode}. Request Id : ${response.ResponseMetadata.RequestId}";
                context.LogError(message);
                await SNSNotify(message, context);
                throw new Exception(message);
            }

            // Let this throw since it logs all errors already
            List<string> arns = await CheckForTasksOnContainerInstanceAsync(ecsCluster, listResponse.ContainerInstanceArns.First(), context);

            // If there are no tasks on the container instance go ahead and complete
            // the hook
            if (!arns.Any())
            {
                context.LogInfo("No tasks remaining on container instance, completing lifecycle hook.");

                // Allow this to throw since it logs and reports errors
                CompleteLifecycleActionResponse hookResponse = await CompleteLifecycleHookAsync(
                    cwEvent.Detail.AutoScalingGroupName,
                    cwEvent.Detail.EC2InstanceId,
                    cwEvent.Detail.LifecycleActionToken,
                    cwEvent.Detail.LifecycleHookName,
                    "CONTINUE",
                    context);
            }
            else
            {
                context.LogInfo($"{arns.Count} tasks remaining on container instance, will wait for them all to finish draining.");
            }

            context.LogInfo("Function complete.");
        }

        /// <summary>
        /// Given the max container size from the environment variables, it queries the instance type used in the cluster
        /// and determines the maximum number of containers that could be scheduled on any node. Then, it evaluates the current 
        /// CPU and memory utilization of all of the nodes and determines how many more containers could be scheduled on the
        /// cluster.
        /// </summary>
        /// <param name="cwEvent"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public async System.Threading.Tasks.Task PutSchedulableContainerMetricAsync(ScheduledEvent cwEvent, ILambdaContext context)
        {
            ListContainerInstancesRequest listRequest = new ListContainerInstancesRequest()
            {
                Cluster = ecsCluster,
                Status = ContainerInstanceStatus.ACTIVE
            };

            List<string> containerInstances = new List<string>();

            do
            {
                ListContainerInstancesResponse listResponse;
                try
                {
                    listResponse = await ecsClient.ListContainerInstancesAsync(listRequest);
                }
                catch (Exception e)
                {
                    context.LogError(e);
                    await SNSNotify($"{e.GetType().FullName} : {e.Message}", context);
                    throw e;
                }

                if (listResponse.HttpStatusCode != HttpStatusCode.OK)
                {
                    string message = $"Failed to list ECS container instances with status code ${(int)listResponse.HttpStatusCode} ${listResponse.HttpStatusCode}. Request Id : ${listResponse.ResponseMetadata.RequestId}";
                    context.LogError(message);
                    await SNSNotify(message, context);
                    throw new Exception(message);
                }

                containerInstances.AddRange(listResponse.ContainerInstanceArns);
                listRequest.NextToken = listResponse.NextToken;

            } while (listRequest.NextToken != null);

            List<Tuple<string, string, int>> schedulableContainersByInstance = new List<Tuple<string, string, int>>();
            int totalSchedulable = 0;

            foreach (List<string> chunk in containerInstances.Chunk(100))
            {
                DescribeContainerInstancesRequest request = new DescribeContainerInstancesRequest()
                {
                    Cluster = ecsCluster,
                    ContainerInstances = chunk
                };

                DescribeContainerInstancesResponse response;

                try
                {
                    response = await ecsClient.DescribeContainerInstancesAsync(request);
                }
                catch (Exception e)
                {
                    context.LogError(e);
                    await SNSNotify($"{e.GetType().FullName} : {e.Message}", context);
                    throw e;
                }

                if (response.HttpStatusCode != HttpStatusCode.OK)
                {
                    string message = $"Failed to describe ECS container instances with status code ${(int)response.HttpStatusCode} ${response.HttpStatusCode}. Request Id : ${response.ResponseMetadata.RequestId}";
                    context.LogError(message);
                    await SNSNotify(message, context);
                    throw new Exception(message);
                }

                if (response.Failures != null && response.Failures.Any())
                {
                    StringBuilder sb = new StringBuilder();

                    foreach (Failure fail in response.Failures)
                    {
                        string message = $"Failed to describe container instance {fail.Arn}. Reason: ${fail.Reason}";
                        context.LogError(message);
                        sb.AppendLine(message);
                    }

                    throw new Exception(sb.ToString());
                }

                foreach (Amazon.ECS.Model.ContainerInstance item in response.ContainerInstances)
                {
                    Amazon.ECS.Model.Resource cpu = item.RemainingResources.First(x => x.Name.Equals("CPU", StringComparison.OrdinalIgnoreCase));
                    Amazon.ECS.Model.Resource memory = item.RemainingResources.First(x => x.Name.Equals("MEMORY", StringComparison.OrdinalIgnoreCase));

                    TryGetResourceValueAsDouble(cpu, context, out double remainingCpu);
                    TryGetResourceValueAsDouble(memory, context, out double remainingMemory);

                    int schedulable = (int)Math.Floor(Math.Min((remainingCpu / maxCPUUnits), (remainingMemory / maxMemory)));

                    schedulableContainersByInstance.Add(new Tuple<string, string, int>(item.Ec2InstanceId, item.ContainerInstanceArn, schedulable));

                    totalSchedulable += schedulable;
                }
            }

            PutMetricDataResponse metricResponse;

            try
            {
                List<MetricDatum> metrics = new List<MetricDatum>()
                {
                    new MetricDatum()
                    {
                        Unit =  StandardUnit.Count,
                        MetricName = "SchedulableContainers",
                        Value = totalSchedulable,
                        StorageResolution = 60,
                        Dimensions = new List<Dimension>()
                        {
                            new Dimension()
                            {
                                Name = "ClusterName",
                                Value = ecsCluster
                            }
                        }
                    }
                };

                metrics.AddRange(schedulableContainersByInstance.SelectMany(x =>
                {
                    return new List<MetricDatum>(){
                        new MetricDatum()
                        {
                            Unit = StandardUnit.Count,
                            MetricName = "SchedulableContainers",
                            Value = x.Item3,
                            StorageResolution = 60,
                            Dimensions = new List<Dimension>()
                            {
                                new Dimension()
                                {
                                    Name = "ClusterName",
                                    Value = ecsCluster
                                },
                                new Dimension()
                                {
                                    Name = "InstanceId",
                                    Value = x.Item1
                                }
                            }
                        },
                        new MetricDatum()
                        {
                            Unit = StandardUnit.Count,
                            MetricName = "SchedulableContainers",
                            Value = x.Item3,
                            StorageResolution = 60,
                            Dimensions = new List<Dimension>()
                            {
                                new Dimension()
                                {
                                    Name = "ClusterName",
                                    Value = ecsCluster
                                },
                                new Dimension()
                                {
                                    Name = "ContainerInstanceArn",
                                    Value = x.Item2
                                }
                            }
                        }
                    };
                }));

                PutMetricDataRequest metric = new PutMetricDataRequest()
                {
                    Namespace = "AWS/ECS",
                    MetricData = metrics
                };

                metricResponse = await cloudWatchClient.PutMetricDataAsync(metric);
            }
            catch (Exception e)
            {
                context.LogError(e);
                await SNSNotify($"{e.GetType().FullName} : {e.Message}", context);
                throw e;
            }

            if (metricResponse.HttpStatusCode != HttpStatusCode.OK)
            {
                string message = $"Failed to put CloudWatch metric data with status code ${(int)metricResponse.HttpStatusCode} ${metricResponse.HttpStatusCode}. Request Id : ${metricResponse.ResponseMetadata.RequestId}";
                context.LogError(message);
                await SNSNotify(message, context);
                throw new Exception(message);
            }

            context.LogInfo("Function complete.");
        }

        /// <summary>
        /// Calculates the number of available schedulable containers that would cause a scale-in action and updates the CloudWatch
        /// Alarm to use that number as the threshold. For example, if a scale-out action is triggered when there are less than 3
        /// schedulable containers and a single node can hold 6 containers, then if 9 or more schedulable containers are available,
        /// then the cluster should scale in. This will leave 3 available schedulable containers, which will not cause a scale-out
        /// action in response to the scale-in.
        /// </summary>
        /// <param name="cwEvent"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public async System.Threading.Tasks.Task DetermineHighThresholdAsync(ScheduledEvent cwEvent, ILambdaContext context)
        {
            string alarmName = Environment.GetEnvironmentVariable("CW_ALARM_NAME");
            string asgName = Environment.GetEnvironmentVariable("ASG_NAME");
            Int32.TryParse(Environment.GetEnvironmentVariable("MIN_THRESHOLD"), out int minThreshold);

            if (minThreshold <= 0)
            {
                string message = $"The minimum threshold env var was either not set or was set to {minThreshold}, which is not valid.";
                context.LogError(message);
                await SNSNotify(message, context);
                throw new Exception(message);
            }

            if (String.IsNullOrEmpty(alarmName))
            {
                string message = $"The CW_ALARM_NAME env var was not set and the function cannot identify which CloudWatch Alarm to update.";
                context.LogError(message);
                await SNSNotify(message, context);
                throw new Exception(message);
            }

            ListContainerInstancesRequest listRequest = new ListContainerInstancesRequest()
            {
                Cluster = ecsCluster,
                Status = ContainerInstanceStatus.ACTIVE,
                MaxResults = 1
            };

            ListContainerInstancesResponse listResponse;

            try
            {
                listResponse = await ecsClient.ListContainerInstancesAsync(listRequest);
            }
            catch (Exception e)
            {
                string message = $"Failed while attempting to list container instances for cluster {ecsCluster}.";
                context.LogError(message, e);
                await SNSNotify($"{message}\n{e.GetType().FullName} : {e.Message}", context);
                throw e;
            }

            if (listResponse.HttpStatusCode != HttpStatusCode.OK)
            {
                string message = $"Failed to list ECS container instances for cluster {ecsCluster} with status code ${(int)listResponse.HttpStatusCode} ${listResponse.HttpStatusCode}. Request Id : ${listResponse.ResponseMetadata.RequestId}";
                context.LogError(message);
                await SNSNotify(message, context);
                throw new Exception(message);
            }

            DescribeContainerInstancesRequest describeRequest = new DescribeContainerInstancesRequest()
            {
                Cluster = ecsCluster,
                ContainerInstances = listResponse.ContainerInstanceArns
            };

            DescribeContainerInstancesResponse describeResponse;

            try
            {
                describeResponse = await ecsClient.DescribeContainerInstancesAsync(describeRequest);
            }
            catch (Exception e)
            {
                string message = $"Failed while attempting to describe container instances {String.Join(", ", describeRequest.ContainerInstances)} for cluster {ecsCluster}.";
                context.LogError(message, e);
                await SNSNotify($"{message}\n{e.GetType().FullName} : {e.Message}", context);
                throw e;
            }

            if (describeResponse.HttpStatusCode != HttpStatusCode.OK)
            {
                string message = $"Failed to describe ECS container instances {String.Join(", ", describeRequest.ContainerInstances)} for cluster {ecsCluster} with status code ${(int)describeResponse.HttpStatusCode} ${describeResponse.HttpStatusCode}. Request Id : ${describeResponse.ResponseMetadata.RequestId}";
                context.LogError(message);
                await SNSNotify(message, context);
                throw new Exception(message);
            }

            Amazon.ECS.Model.Resource cpu = describeResponse.ContainerInstances.First().RegisteredResources.First(x => x.Name.Equals("CPU", StringComparison.OrdinalIgnoreCase));
            Amazon.ECS.Model.Resource memory = describeResponse.ContainerInstances.First().RegisteredResources.First(x => x.Name.Equals("MEMORY", StringComparison.OrdinalIgnoreCase));

            TryGetResourceValueAsDouble(cpu, context, out double maxCpuPerInstance);
            TryGetResourceValueAsDouble(memory, context, out double maxMemoryPerInstance);

            // Total containers schedulable on a single container instance
            int totalSchedulable = (int)Math.Floor(Math.Min((maxCpuPerInstance / maxCPUUnits), (maxMemoryPerInstance / maxMemory)));

            context.LogInfo($"The max total schedulable containers per instance is {totalSchedulable}.");
            context.LogInfo($"Max CPU Units per instance is {maxCpuPerInstance}");
            context.LogInfo($"Max Memory per instance is {maxMemoryPerInstance}");
            context.LogInfo($"Max container CPU Units is {maxCPUUnits}");
            context.LogInfo($"Max container memory is {maxMemory}");

            // Add the minimum threshold to this amount
            totalSchedulable += minThreshold;

            context.LogInfo($"Determined that the appropriate high threshold is {totalSchedulable} schedulable containers.");

            // Now we have the amount of containers that available to be scheduled that would allow
            // us to scale in by 1 node and still maintain the minimum threshold so we don't immediately
            // scale back out (and then in again, and out again, etc)

            DescribeAlarmsRequest getAlarm = new DescribeAlarmsRequest()
            {
                AlarmNames = new List<string>() { alarmName },
                MaxRecords = 1
            };

            DescribeAlarmsResponse getAlarmResponse;

            try
            {
                getAlarmResponse = await cloudWatchClient.DescribeAlarmsAsync(getAlarm);
            }
            catch (Exception e)
            {
                string message = $"Failed while attempting to describe alarm {alarmName} for cluster {ecsCluster}.";
                context.LogError(message, e);
                await SNSNotify($"{message}\n{e.GetType().FullName} : {e.Message}", context);
                throw e;
            }

            if (getAlarmResponse.HttpStatusCode != HttpStatusCode.OK)
            {
                string message = $"Failed while attempting to describe alarm {alarmName} for cluster {ecsCluster} with status code ${(int)getAlarmResponse.HttpStatusCode} ${getAlarmResponse.HttpStatusCode}. Request Id : ${getAlarmResponse.ResponseMetadata.RequestId}";
                context.LogError(message);
                await SNSNotify(message, context);
                throw new Exception(message);
            }

            if (!getAlarmResponse.MetricAlarms.Any())
            {
                string message = $"Found no alarms matching {alarmName}.";
                context.LogError(message);
                await SNSNotify(message, context);
                throw new Exception(message);
            }

            MetricAlarm alarm = getAlarmResponse.MetricAlarms.First();

            PutMetricAlarmRequest alarmRequest = CopyProperties<PutMetricAlarmRequest>(alarm, "DatapointsToAlarm");
            alarmRequest.Threshold = totalSchedulable;
            
            PutMetricAlarmResponse alarmResponse;

            try
            {
                alarmResponse = await cloudWatchClient.PutMetricAlarmAsync(alarmRequest);
            }
            catch (Exception e)
            {
                string message = $"Failed while attempting to update alarm {alarmName} for cluster {ecsCluster}.";
                context.LogError(message, e);
                await SNSNotify($"{message}\n{e.GetType().FullName} : {e.Message}", context);
                throw e;
            }

            if (alarmResponse.HttpStatusCode != HttpStatusCode.OK)
            {
                string message = $"Failed while attempting to update alarm {alarmName} for cluster {ecsCluster} with status code ${(int)alarmResponse.HttpStatusCode} ${alarmResponse.HttpStatusCode}. Request Id : ${alarmResponse.ResponseMetadata.RequestId}";
                context.LogError(message);
                await SNSNotify(message, context);
                throw new Exception(message);
            }

            context.LogInfo("Function complete.");
        }

        #endregion

        #region Private Methods

        /// <summary>
        /// Checks to see if there are active tasks on a container instance
        /// </summary>
        /// <param name="cluster"></param>
        /// <param name="containerInstanceArn"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        private static async System.Threading.Tasks.Task<List<string>> CheckForTasksOnContainerInstanceAsync(string cluster, string containerInstanceArn, ILambdaContext context)
        {
            ListTasksRequest request = new ListTasksRequest()
            {
                Cluster = cluster,
                ContainerInstance = containerInstanceArn,
            };

            ListTasksResponse response;

            List<string> arns = new List<string>();

            do
            {
                try
                {
                    response = await ecsClient.ListTasksAsync(request);
                }
                catch (Exception e)
                {
                    string message = "Failed to list ECS tasks.";
                    context.LogError(message, e);
                    await SNSNotify($"{message}\n{e.GetType().FullName} : {e.Message}", context);
                    throw e;
                }

                if (response.HttpStatusCode != HttpStatusCode.OK)
                {
                    string message = $"Failed to list ECS tasks with status code ${(int)response.HttpStatusCode} ${response.HttpStatusCode}. Request Id : ${response.ResponseMetadata.RequestId}";
                    context.LogError(message);
                    await SNSNotify(message, context);
                    throw new Exception(message);
                }

                arns.AddRange(response.TaskArns);
                request.NextToken = response.NextToken;

            } while (response.NextToken != null);

            return arns;
        }

        /// <summary>
        /// Small wrapper around the CompleteLifecycleActionAsync API call to include logging
        /// and SNS notifications
        /// </summary>
        /// <param name="request"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        private static async System.Threading.Tasks.Task<CompleteLifecycleActionResponse> CompleteLifecycleHookAsync(
            CompleteLifecycleActionRequest request,
            ILambdaContext context
            )
        {
            CompleteLifecycleActionResponse lifecycleResponse;

            try
            {
                lifecycleResponse = await autoScalingClient.CompleteLifecycleActionAsync(request);
            }
            catch (Exception e)
            {
                string message = $"Failed to complete lifecycle hook action for instance {request.InstanceId} in ASG {request.AutoScalingGroupName}.";
                context.LogError(message, e);
                await SNSNotify($"{message}\n{e.GetType().FullName} : {e.Message}", context);
                throw e;
            }

            if (lifecycleResponse.HttpStatusCode != HttpStatusCode.OK)
            {
                string message = $"Failed to complete lifecycle hook with status code ${(int)lifecycleResponse.HttpStatusCode} ${lifecycleResponse.HttpStatusCode}. Request Id : ${lifecycleResponse.ResponseMetadata.RequestId}";
                context.LogError(message);
                await SNSNotify(message, context);
                throw new Exception(message);
            }

            return lifecycleResponse;
        }

        /// <summary>
        /// Small wrapper to create a CompleteLifecycleActionRequest and execute the API call to include
        /// logging and SNS notifications
        /// </summary>
        /// <param name="autoScalingGroupName"></param>
        /// <param name="instanceId"></param>
        /// <param name="lifecycleActionToken"></param>
        /// <param name="lifecycleHookName"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        private static async System.Threading.Tasks.Task<CompleteLifecycleActionResponse> CompleteLifecycleHookAsync(
            string autoScalingGroupName,
            string instanceId,
            Guid lifecycleActionToken,
            string lifecycleHookName,
            string lifecycleActionResult,
            ILambdaContext context
            )
        {
            CompleteLifecycleActionRequest lifecycleRequest = new CompleteLifecycleActionRequest()
            {
                AutoScalingGroupName = autoScalingGroupName,
                InstanceId = instanceId,
                LifecycleActionResult = lifecycleActionResult,
                LifecycleHookName = lifecycleHookName
            };

            if (lifecycleActionToken != Guid.Empty)
            {
                lifecycleRequest.LifecycleActionToken = lifecycleActionToken.ToString();
            }

            return await CompleteLifecycleHookAsync(lifecycleRequest, context);
        }

        /// <summary>
        /// Small wrapper to create a CompleteLifecycleActionRequest and execute the API call to include
        /// logging and SNS notifications
        /// </summary>
        /// <param name="autoScalingGroupName"></param>
        /// <param name="instanceId"></param>
        /// <param name="lifecycleHookName"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        private static async System.Threading.Tasks.Task<CompleteLifecycleActionResponse> CompleteLifecycleHookAsync(
            string autoScalingGroupName,
            string instanceId,
            string lifecycleHookName,
            string lifecycleActionResult,
            ILambdaContext context
            )
        {
            return await CompleteLifecycleHookAsync(autoScalingGroupName, instanceId, Guid.Empty, lifecycleHookName, lifecycleActionResult, context);
        }

        /// <summary>
        /// Attempts the get the resource value of an ECS node as a double
        /// </summary>
        /// <param name="resource"></param>
        /// <param name="context"></param>
        /// <param name="value"></param>
        /// <returns></returns>
        private bool TryGetResourceValueAsDouble(Amazon.ECS.Model.Resource resource, ILambdaContext context, out double value)
        {
            switch (resource.Type)
            {
                case "INTEGER":
                    {
                        value = resource.IntegerValue;
                        return true;
                    }
                case "LONG":
                    {
                        value = resource.LongValue;
                        return true;
                    }
                case "DOUBLE":
                    {
                        value = resource.DoubleValue;
                        return true;
                    }
                case "STRINGSET":
                    {
                        context.LogError($"The type 'STRINGSET' is not valid for the resource:\n${JsonConvert.SerializeObject(resource)}");
                        value = 0;
                        return false;
                    }
                default:
                    {
                        context.LogError($"The type '${resource.Type} was not recognized for the resource:\n${JsonConvert.SerializeObject(resource)}");
                        value = 0;
                        return false;
                    }
            }
        }

        /// <summary>
        /// If configured, sends an SNS notification to a topic
        /// </summary>
        /// <param name="message"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        private static async System.Threading.Tasks.Task SNSNotify(string message, ILambdaContext context)
        {
            if (!String.IsNullOrEmpty(snsTopic))
            {
                try
                {
                    PublishResponse response = await snsClient.PublishAsync(snsTopic, message, emailSubject);

                    if (response.HttpStatusCode != HttpStatusCode.OK)
                    {
                        context.LogError($"Failed to send SNS notification with status code {(int)response.HttpStatusCode}.");
                    }
                }
                catch (Exception e)
                {
                    context.LogError("Failed to send SNS notification.", e);
                }
            }
        }

        /// <summary>
        /// Copies the properties from the provided source object into a new object of the specified destination
        /// type. The properties that are copied are matched by property name, so if both objects have a "Name" property for example,
        /// then the value in the source is set on the destination "Name" property. Properties that cannot be set or 
        /// assigned to in the destination or cannot be read from the source are omitted.
        /// </summary>
        /// <typeparam name="TDestination">The destination type to construct and assign to.</typeparam>
        /// <param name="source">The source object whose properties will be copied</param>
        /// <returns>A new object with the copied property values from matching property names in the source.</returns>
        private static TDestination CopyProperties<TDestination>(object source, params string[] propertiesToIgnore) where TDestination : class, new()
        {
            IEnumerable<PropertyInfo> Properties = source.GetType().GetRuntimeProperties();

            TDestination Destination = new TDestination();
            Type DestinationType = Destination.GetType();

            if (propertiesToIgnore == null)
            {
                propertiesToIgnore = new string[0];
            }

            foreach (PropertyInfo Info in Properties.Where(x => !propertiesToIgnore.Contains(x.Name, StringComparer.OrdinalIgnoreCase)))
            {
                try
                {
                    // If the property can't be read, just move on to the
                    // next item in the foreach loop
                    if (!Info.CanRead)
                    {
                        continue;
                    }

                    PropertyInfo DestinationProperty = DestinationType.GetProperty(Info.Name);

                    // If the destination is null (property doesn't exist on the object), 
                    // can't be written, or isn't assignable from the source, move on to the next
                    // property in the foreach loop
                    if (DestinationProperty == null ||
                        !DestinationProperty.CanWrite ||
                        (DestinationProperty.GetSetMethod() != null && (DestinationProperty.GetSetMethod().Attributes & MethodAttributes.Static) != 0) ||
                        !DestinationProperty.PropertyType.IsAssignableFrom(Info.PropertyType))
                    {
                        continue;
                    }

                    DestinationProperty.SetValue(Destination, Info.GetValue(source, null));
                }
                catch (Exception)
                {
                    continue;
                }
            }

            return Destination;
        }

        #endregion
    }
}

