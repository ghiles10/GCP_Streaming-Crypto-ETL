provider "google" {

  project = var.project
  region  = var.region
  zone    = var.zone
}

resource "google_compute_firewall" "port_rules" {

  project     = var.project
  name        = "port-kafka"
  network     = var.network
  description = "Opens port 9092 in the Kafka VM for Spark cluster to connect"

  allow {
    protocol = "tcp"
    ports    = ["9092"]
  }

  source_ranges = ["0.0.0.0/0"]
  target_tags   = ["kafka-finance"]
}

resource "google_compute_instance" "kafka_producer-vm" {

  name                      = "finance-api-kafka-instance"
  machine_type              = "e2-medium"
  tags                      = ["kafka-finance"]
  allow_stopping_for_update = true

  boot_disk {
    initialize_params {
      image = var.vm_image
      size  = 20
    }
  }

  network_interface {
    network = var.network
    access_config {
    }
  }
}

resource "google_storage_bucket" "kafka-finance-data" {
  name          = var.bucket
  location      = var.region
  force_destroy = true

  uniform_bucket_level_access = true

  lifecycle_rule {
    action {
      type = "Delete"
    }
    condition {
      age = 10 # days
    }
  }
}

resource "google_dataproc_cluster" "spark_cluster" {
  name   = "spark-cluster-finance"
  region = var.region

  cluster_config {

    staging_bucket = var.bucket

    gce_cluster_config {
      network = var.network
      zone    = var.zone
    }

    master_config {
      num_instances = 1
      machine_type  = "n2-standard-2"
      disk_config {
        boot_disk_type    = "pd-ssd"
        boot_disk_size_gb = 30
      }
    }

    worker_config {
      num_instances = 2
      machine_type  = "n2-standard-2"
      disk_config {
        boot_disk_size_gb = 30
      }
    }

    software_config {
      image_version = "2.0-debian10"
      override_properties = {
        "dataproc:dataproc.allow.zero.workers" = "true"
      }
      optional_components = ["JUPYTER"]
    }

  }

}