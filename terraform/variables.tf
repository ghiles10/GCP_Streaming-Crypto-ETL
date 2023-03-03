variable "project" {
  description = "id GCP project"
  type        = string
  default     = "data-engineering-streaming"
}

variable "region" {
  description = "project regio,"
  type        = string
  default     = "us-central1"
}

variable "zone" {
  description = "project zone"
  type        = string
  default     = "us-central1-c"
}

variable "network" {

  description = "network for comoute products"
  type        = string
  default     = "default"
}


variable "vm_image" {
  description = "image os"
  type        = string
  default     = "ubuntu-os-cloud/ubuntu-2004-lts"
}

variable "bucket" {
  description = "bucket name"
  type        = string
  default     = "kafka-finance-data"
}