IMAGE ?= budstudio/minio-downloader
TAG   ?= 0.4.0

.PHONY: image image-multiarch push test

image:
	docker build -t $(IMAGE):$(TAG) -t $(IMAGE):latest .

image-multiarch:
	docker buildx build --platform linux/amd64,linux/arm64 \
		-t $(IMAGE):$(TAG) -t $(IMAGE):latest --push .

push: image
	docker push $(IMAGE):$(TAG)
	docker push $(IMAGE):latest

test:
	python -m unittest discover -p 'test_*.py' -v
