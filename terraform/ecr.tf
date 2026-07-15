resource "aws_ecr_repository" "hydra_lambda" {
  name                 = "hydra-data-factory-lambda"
  image_tag_mutability = "MUTABLE"
  force_delete         = true

  tags = local.common_tags
}
