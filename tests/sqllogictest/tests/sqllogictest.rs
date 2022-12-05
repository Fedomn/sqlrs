use libtest_mimic::{Arguments, Trial};
use sqllogictest_test::{test_run, test_run_v2};

fn main() {
    const SLT_PATTERN: &str = "../slt/**/*.slt";

    let args = Arguments::from_args();
    let mut tests = vec![];

    let slt_files = glob::glob(SLT_PATTERN).expect("failed to find slt files");
    for slt_file in slt_files {
        let filepath = slt_file.expect("failed to read slt file");
        let filename = filepath
            .file_stem()
            .expect("failed to get file name")
            .to_str()
            .unwrap()
            .to_string();
        let filepath = filepath.to_str().unwrap().to_string();

        let test = Trial::test(filename, move || {
            test_run(filepath.as_str());
            test_run_v2(filepath.as_str());
            Ok(())
        });

        tests.push(test);
    }

    libtest_mimic::run(&args, tests).exit();
}
