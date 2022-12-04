pipeline {
  agent any

  stages {
    stage("Setup") {
      steps {
        checkout scm

        // During CI runs, the user is different from the owner of the directories, which blocks the execution of git
        // commands since the fix of the git vulnerability CVE-2022-24765. git commands can then only be executed if
        // the corresponding directories are added as safe directories.
        sh '''
        git config --global --add safe.directory $WORKSPACE
        # Get the paths of the submodules; for each path, add it as a git safe.directory
        grep path .gitmodules | sed 's/.*=//' | xargs -n 1 -I '{}' git config --global --add safe.directory $WORKSPACE/'{}'
        '''

        sh "./install_dependencies.sh"

        cmake = 'cmake -DCI_BUILD=ON'

        // We don't use unity builds with GCC 9 as it triggers https://github.com/google/googletest/issues/3552
        unity = '-DCMAKE_UNITY_BUILD=ON'
        */
        // With Hyrise, we aim to support the most recent compiler versions and do not invest a lot of work to
        // support older versions. We test the oldest LLVM version shipped with Ubuntu 22.04 (i.e., LLVM 11) and
        // GCC 9 (oldest version supported by Hyrise). We execute at least debug runs for them.
        // If you want to upgrade compiler versions, please update install_dependencies.sh,  DEPENDENCIES.md, and
        // the documentation (README, Wiki).
        /*
        clang = '-DCMAKE_C_COMPILER=clang -DCMAKE_CXX_COMPILER=clang++'
        clang11 = '-DCMAKE_C_COMPILER=clang-11 -DCMAKE_CXX_COMPILER=clang++-11'
        gcc = '-DCMAKE_C_COMPILER=gcc -DCMAKE_CXX_COMPILER=g++'
        gcc9 = '-DCMAKE_C_COMPILER=gcc-9 -DCMAKE_CXX_COMPILER=g++-9'

        debug = '-DCMAKE_BUILD_TYPE=Debug'
        release = '-DCMAKE_BUILD_TYPE=Release'
        relwithdebinfo = '-DCMAKE_BUILD_TYPE=RelWithDebInfo'

        // jemalloc's autoconf operates outside of the build folder (#1413). If we start two cmake instances at the same time, we run into conflicts.
        // Thus, run this one (any one, really) first, so that the autoconf step can finish in peace.

        sh "mkdir clang-debug && cd clang-debug &&                                                   ${cmake} ${debug}          ${clang}  ${unity}  .. && make -j libjemalloc-build"

        // Configure the rest in parallel
        sh "mkdir clang-debug-tidy && cd clang-debug-tidy &&                                         ${cmake} ${debug}          ${clang}            -DENABLE_CLANG_TIDY=ON .. &\
        mkdir clang-debug-unity-odr && cd clang-debug-unity-odr &&                                   ${cmake} ${debug}          ${clang}   ${unity} -DCMAKE_UNITY_BUILD_BATCH_SIZE=0 .. &\
        mkdir clang-debug-disable-precompile-headers && cd clang-debug-disable-precompile-headers && ${cmake} ${debug}          ${clang}            -DCMAKE_DISABLE_PRECOMPILE_HEADERS=On .. &\
        mkdir clang-debug-addr-ub-sanitizers && cd clang-debug-addr-ub-sanitizers &&                 ${cmake} ${debug}          ${clang}            -DENABLE_ADDR_UB_SANITIZATION=ON .. &\
        mkdir clang-release-addr-ub-sanitizers && cd clang-release-addr-ub-sanitizers &&             ${cmake} ${release}        ${clang}   ${unity} -DENABLE_ADDR_UB_SANITIZATION=ON .. &\
        mkdir clang-release && cd clang-release &&                                                   ${cmake} ${release}        ${clang}            .. &\
        mkdir clang-relwithdebinfo-thread-sanitizer && cd clang-relwithdebinfo-thread-sanitizer &&   ${cmake} ${relwithdebinfo} ${clang}            -DENABLE_THREAD_SANITIZATION=ON .. &\
        mkdir gcc-debug && cd gcc-debug &&                                                           ${cmake} ${debug}          ${gcc}              .. &\
        mkdir gcc-release && cd gcc-release &&                                                       ${cmake} ${release}        ${gcc}              .. &\
        mkdir clang-11-debug && cd clang-11-debug &&                                                 ${cmake} ${debug}          ${clang11}          .. &\
        mkdir gcc-9-debug && cd gcc-9-debug &&                                                       ${cmake} ${debug}          ${gcc9}             .. &\
        wait"*/
      }        
    }
  }
}