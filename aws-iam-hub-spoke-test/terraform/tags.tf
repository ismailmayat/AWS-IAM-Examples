# Tags applied to every taggable AWS resource through the aws provider's
# default_tags. Set them in terraform.tfvars (see terraform.tfvars.example).
variable "tags" {
  description = "Tags applied to every AWS resource created by this module."
  type        = map(string)
  default     = {}
}
