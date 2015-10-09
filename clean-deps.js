let fs = require('fs');
let file = require('file');
let _ = require('lodash');

let pkg = require(process.cwd() + '/package.json');
// We want a fresh beginning
let newPkg = _.cloneDeep(pkg);
newPkg.dependencies = {};

let dep = _.keys(pkg.dependencies);
let devDep = _.keys(pkg.devDependencies);

// find non-test js files
let programFiles = [];
file.walkSync('.', function(dirName, dirPath, files, dirs) {
  if (!dirName.match(/^node_modules/) &&
      !dirName.match(/^.git/) &&
      !dirName.match(/^lib/) &&
      !dirName.match(/^test/) &&
      !dirName.match(/^.test/)){
    for (let x of files || []) {
      if (x.match(/.js$/)) {
        programFiles.push(dirName + '/' + x);
      }
    }
  }
});

// find test js files
let testFiles = [];
file.walkSync('.', function(dirName, dirPath, files, dirs) {
  if (dirName.match(/^test/)){
    for (let x of files || []) {
      if (x.match(/.js$/)) {
        testFiles.push(dirName + '/' + x);
      }
    }
  }
});

let packages = [];
let requireRegex = /require\(['"]([^'"]*)['"]\)/gm
for (let pf of programFiles) {
  let data = fs.readFileSync(pf);
  let m;
  while (m = requireRegex.exec(data)) {
    if (!packages.includes(m[1])) {
      packages.push(m[1]);
    }
  }

}
packages = packages.filter(x => dep.includes(x));

let testPackages = [];
for (let tf of testFiles) {
  let data = fs.readFileSync(tf);
  let m;
  while (m = requireRegex.exec(data)) {
    let name = m[1];
    if (m[1] === '../') {
      name = 'taskcluster-base';
      let before = data.slice(0, m.index + 9);
      let after = data.slice(m.index + 12);
      fs.writeFileSync(tf, before + name + after);
      newPkg.devDependencies[name] = '0.8.6';
    }

    if (!packages.includes(name) && !testPackages.includes(name)) {
      testPackages.push(name);
    }
  }

}
testPackages = testPackages.filter(x => devDep.includes(x));

for (let x of packages) {
  newPkg.dependencies[x] = pkg.dependencies[x];
}

for (let x of testPackages) {
  newPkg.devDependencies[x] = pkg.devDependencies[x] || pkg.dependencies[x];
}

fs.writeFileSync('package.json', JSON.stringify(newPkg, null, 2));

