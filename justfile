doctor:
    cargo xtask doctor

setup-browser:
    cargo xtask setup browser

check:
    cargo xtask check

release-check:
    cargo xtask check publish

ci-pr:
    cargo xtask ci pr-fast --keep-artifacts

ci-browser:
    cargo xtask ci browser --keep-artifacts

ci-integration:
    cargo xtask ci integration --keep-artifacts

ci-services:
    cargo xtask ci services --keep-artifacts

ci-nightly:
    cargo xtask ci nightly --keep-artifacts

ci-publish:
    cargo xtask ci publish --keep-artifacts

e2e:
    cargo xtask e2e smoke

e2e-mixed:
    cargo xtask e2e mixed

e2e-services:
    cargo xtask e2e services

browser-smoke:
    cargo xtask browser smoke

browser-trainer:
    cargo xtask browser trainer

browser-real:
    cargo xtask browser real --profile real-browser

adversarial-smoke:
    cargo xtask adversarial smoke

adversarial-matrix:
    cargo xtask adversarial matrix

adversarial-chaos seed="12345":
    cargo xtask adversarial chaos --seed {{seed}}

stress peers="8":
    cargo xtask stress multiprocess --peers {{peers}}

chaos seed="12345":
    cargo xtask stress chaos --seed {{seed}}

bench:
    cargo xtask bench core

bench-robust:
    cargo xtask bench robust
