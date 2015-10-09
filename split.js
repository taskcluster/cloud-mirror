let fs = require('fs');
let tcBasePkg = require('./taskcluster-base/package.json');
let Promise = require('promise');
let _exec = require('child_process').exec;
let _ = require('lodash');
let path = require('path');

function exec(...x) {
  return new Promise(function(resolve, reject) {
    _exec.apply(null, x.concat(function(err, stdout, stderr) {
      if (err) reject(err);
      else resolve({stdout: stdout, stderr: stderr});
    }));
  });
}

async function createSubProject(baseRepo, newName, config) {
  await exec('git clone ' + baseRepo + ' ' + newName);
  await filterUnwantedFiles(newName, config.files);
  await exec('git -C ' + newName + ' remote rm origin');
  await exec('git -C ' + newName + ' remote add origin github.com:taskcluster/' + newName);
  let pkg = _.cloneDeep(tcBasePkg);
  pkg.name = newName;
  pkg.description = newName;
  pkg.repository.url = "https://github.com/taskcluster/" + newName + ".git";

  // Note that if we move all the files out of a directory, we leave them
  // there.  This is not a big deal because GIT doesn't track directories,
  // only files, so it's just a minor inconvenience.
  for (let x of config.moves) {
    let dir = path.dirname(x[1]);
    await exec('(cd ' + newName + ' && mkdir -p ' + dir + ')');
    await exec('git -C ' + newName + ' mv ' + x[0] + ' ' + x[1]);
  }

  // Got through the pkgOverwrites
  for (let x of config.pkgOverwrite) {
    _.set(pkg, x[0], x[1]);
  }

  for (let x of _.keys(config.newFiles)) {
    let f = path.dirname(x);
    await exec('(cd ' + newName + ' && mkdir -p ' + f + ')');
    let data = config.newFiles[x];
    if (typeof data === 'object') {
      data = JSON.stringify(data, null, 2) + '\n';
    } else if (Array.isArray(data)) {
      data = data.join('\n') + '\n';
    }
    fs.writeFileSync(newName + '/' + x, data);
  }

  for (let x of config.scripts) {
    await exec('(cd ' + newName + ' && ' + x + ')');
  }

  fs.writeFileSync(newName + '/package.json', JSON.stringify(pkg, null, 2));
  await exec('git -C ' + newName + ' add package.json');
  await exec('git -C ' + newName + ' commit -va -m "splitting from taskcluster-base"');
  await exec('git -C ' + newName + ' tag SPLIT_FROM_TC_BASE');
  // TODO Maybe we could use Hub to do hub create taskcluster/$newName -d $newName
}

// List all tracked files in the current repository
async function lsFiles(repo) {
  let output = await exec('git -C ' + repo + ' ls');
  output = output.stdout.split('\n').filter(x => !!x);
  return output;
}

// Given a list of files that we do want, filter out the
// ones that aren't on that list
async function findUnwantedFiles(repo, wantedFiles) {
  let allFiles = await lsFiles(repo);
  let unwantedFiles = allFiles.filter(x => !wantedFiles.includes(x));
  for (let x of wantedFiles) {
    if (!fs.existsSync(repo + '/' + x)) {
      throw new Error('Wanted file must exist!');
    }
  }
  return unwantedFiles;
}

async function filterUnwantedFiles(repo, wantedFiles) {
  let unwantedFiles = await findUnwantedFiles(repo, wantedFiles);
  let filter = unwantedFiles.join(' ');
  await(exec('git -C ' + repo + ' filter-branch --prune-empty --tree-filter "rm -f ' + filter + '"'));
}


/**
 * Configuration Format:
 *
 * {
 *   newProjectName: {
 *     files: [
 *     ],
 *     moves: [
 *     ],
 *     pkgOverwrite: [
 *     ],
 *     scripts: [
 *     ],
 *     newFiles: [
 *     ],
 *   }
 * }
 *
 * files are the files that should be left in the repo, moves are things
 * to git move (array, [from, to]) and pkgOverwrite are things to write
 * over the package.json when generating it
 *
 * first we clone the repo, next we filter out the files we don't want
 * then we do the moves then we do the packageOverwrites
 *
 * Things to do!
 *   - pair down dependencies
 *   - edit unit test suites to switch require('../') with require('taskcluster-base')
 *   - install taskcluster-base as a dev-dep
 */

async function main() {
  try {
    let config = require(process.argv[2] || './config.json');
    for (let x of _.keys(config)) {
      await createSubProject('taskcluster-base', x, config[x]);
    }
  } catch (e) {
    console.log(e.stack || e);
    process.exit(-1);
  }
};


main();
