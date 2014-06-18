all: mimetypes.json

mimetypes.json:
	python gen_mimes.py | jq -S . > $@

clean:
	rm -f mimetypes.json
