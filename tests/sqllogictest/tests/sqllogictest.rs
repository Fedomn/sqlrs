use libtest_mimic::{run_tests, Arguments, Outcome, Test};
use sqllogictest_test::test_run;

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
        tests.push(Test {
            name: filename,
            kind: "".into(),
            is_ignored: false,
            is_bench: false,
            data: filepath,
        })
    }

    run_tests(&args, tests, |test| {
        let filepath = &test.data;
        test_run(filepath);
        Outcome::Passed
    })
    .exit();
}
