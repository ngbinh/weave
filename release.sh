#!/bin/sh

# Release script for releasing Weave artifacts
RELEASE=$1

# Fetch latest from develop
git fetch origin

# Checkout develop
git checkout develop

# Hard reset develop to origin/develop
git reset --hard origin/develop

# Clean everything
mvn clean

# Generate the release version
VERSION=`mvn org.apache.maven.plugins:maven-help-plugin:evaluate -Dexpression=project.version | grep -v '^\[' | sed 's/-SNAPSHOT$//g'`

if [ "x$VERSION" == "x" ]
then
    echo "Failed to generate release version."
    exit 1
fi

if [[ "$VERSION" == *SNAPSHOT* ]]
then
    echo "Invalid release version: $VERSION."
    exit 1
fi

if [ "x$RELEASE" != "--release" ]
then
    echo
    echo "DRY-RUN ONLY"
    echo "Argument --release is not given, this run is a dry-run. All changes are made locally only and no artifacts will be published."
    echo "After this run, run \"git reset --hard origin/develop\" and \"git branch -D release/$VERSION\" to remove local changes."
    echo
fi


echo "Current release version is $VERSION. What is the next release version (develop branch would upgrade to <new_version>-SNAPSHOT)? "
read NEW_VERSION

if [ "x$VERSION" == "x$NEW_VERSION" ]
then
    echo "Invalid new version: $NEW_VERSION == $VERSION."
    exit 1
fi

NEW_VERSION="${NEW_VERSION}-SNAPSHOT"

echo "Next release snapshot is $NEW_VERSION, confirm (y/n)?"
read CONFIRM_VERSION

if [ "x$CONFIRM_VERSION" != "xy" ]
then
    echo "Release abort."
    exit 1
fi

echo "Releasing version $VERSION"

# Create release branch
git checkout -b release/$VERSION

# Set release version to pom
mvn versions:set -DnewVersion=$VERSION -DgenerateBackupPoms=false

# Comit version changes
git commit -m "Release version $VERSION artifacts" .

if [ "x$RELEASE" == "--release" ]
then
    # Push to remote release branch
    git push origin release/$VERSION

    # Compile the Hadoop-2.0 version
    mvn compile -P hadoop-2.0,-hadoop-2.2

    # Compile and package Hadoop-2.2 classes
    mvn package -DskipTests=true -P hadoop-2.2

    # Publish artifacts
    mvn deploy -DskipTests=true -P hadoop-2.2
fi

echo "Release completed"

echo "Upgrade to next SNAPSHOT version"

git checkout develop

# Upgrade SNAPSHOT version
mvn versions:set -DnewVersion=$NEW_VERSION -DgenerateBackupPoms=false

# Commit version change
git commit -m "Bump SHAPSHOT version to $NEW_VERSION" .

if [ "x$RELEASE" == "--release" ]
then
    # Push to remote release branch
    git push origin develop

    # Compile the Hadoop-2.0 version
    mvn compile -P hadoop-2.0,-hadoop-2.2

    # Compile and package Hadoop-2.2 classes
    mvn package -DskipTests=true -P hadoop-2.2

    # Publish first SNAPSHOT artifacts
    mvn deploy -DskipTests=true -P hadoop-2.2
fi
