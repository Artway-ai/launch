
PY_SITE=$(shell python -c "import site; print(site.getsitepackages()[0])")

default:
	echo "usage python -m launch ARGS demo.py"

uninstall:
	rm -rf $(PY_SITE)/launch

install: uninstall
	cp -r launch $(PY_SITE)/

