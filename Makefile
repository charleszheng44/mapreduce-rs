.PHONY: clean
clean: clean-output clean-bin

.PHONY: clean-output
clean-output:
	-rm mr-inp-* mr-out-*

.PHONY: clean-bin
clean-bin:
	-rm -rf target
