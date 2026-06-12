variable "aws_region" {
  description = "AWS region for all resources"
  type        = string
  default     = "us-east-1"
}

variable "project_name" {
  description = "Project name prefix used in all resource names"
  type        = string
  default     = "covid-de"
}

variable "suffix" {
  description = "Your unique name suffix for S3 bucket names (must be globally unique)"
  type        = string
  default     = "pavan"   # Change this to your name
}

variable "environment" {
  description = "Deployment environment tag"
  type        = string
  default     = "dev"
}
