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
  machine_type              = "e2-micro"
  tags                      = ["kafka-finance"]
  allow_stopping_for_update = true

  boot_disk {
    initialize_params {
      image = var.vm_image
      size  = 10
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
