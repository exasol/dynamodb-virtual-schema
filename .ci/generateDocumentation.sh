#!/bin/bash

set -e

readonly committer_mail="travis@travis-ci.org"

git clone -b gh-pages "git@github.com:${TRAVIS_REPO_SLUG}.git" schema_docs
cd schema_docs

##### Configure git.
# Set the push default to simple i.e. push only the current branch.
git config --global push.default simple
# Pretend to be a user called "Travis CI".
git config user.name "Travis CI"
git config user.email "$committer_mail"

# go back to first commit
git reset --hard "$(git rev-list --max-parents=0 --abbrev-commit HEAD)"

#generate documentation
readonly schema_path="$TRAVIS_BUILD_DIR/src/main/resources/schemas/edml/v1.json"
bootprint json-schema "$schema_path" ./schema_doc/


if [ -d "schema_doc" ] && [ -f "schema_doc/index.html" ]; then
    echo 'Uploading documentation to the gh-pages branch...'
    # Add everything in this directory (the Doxygen code documentation) to the
    # gh-pages branch.
    # GitHub is smart enough to know which files have changed and which files have
    # stayed the same and will only update the changed files.
    git add --all

    # Commit the added files with a title and description containing the Travis CI
    # build number and the GitHub commit reference that issued this build.
    git commit -m "Deploy code docs to GitHub Pages Travis build: ${TRAVIS_BUILD_NUMBER}" -m "Commit: ${TRAVIS_COMMIT}"

    # Force push to the remote gh-pages branch.
    # The ouput is redirected to /dev/null to hide any sensitive credential data
    # that might otherwise be exposed.
    git push --force "git@github.com:${TRAVIS_REPO_SLUG}.git" > /dev/null 2>&1
else
    echo '' >&2
    echo 'Warning: No documentation (html) files have been found!' >&2
    echo 'Warning: Not going to push the documentation to GitHub!' >&2
    exit 1
fi
