use std::path::Path;

use ort::{
    execution_providers::CPUExecutionProvider,
    session::Session,
    tensor::TensorElementType,
    value::{TensorRef, ValueType},
};

use crate::{FRAME_SAMPLES, SAMPLE_RATE, VadError};

const CONTEXT: usize = 64;
const STATE: usize = 2 * 128;

pub(crate) struct Model {
    session: Session,
    context: [f32; CONTEXT],
    state: [f32; STATE],
}

impl Model {
    pub fn load(path: &Path) -> Result<Self, VadError> {
        let session = (|| {
            Session::builder()?
                .with_execution_providers([CPUExecutionProvider::default().build()])?
                .with_intra_threads(1)?
                .with_inter_threads(1)?
                .commit_from_file(path)
        })()
        .map_err(|source| VadError::Model {
            source: Box::new(source),
        })?;
        validate_contract(&session)?;
        let mut model = Self {
            session,
            context: [0.0; CONTEXT],
            state: [0.0; STATE],
        };
        model
            .infer(&[0; FRAME_SAMPLES])
            .map_err(|source| VadError::Model {
                source: Box::new(source),
            })?;
        model.reset();
        Ok(model)
    }

    pub fn infer(&mut self, pcm: &[i16]) -> Result<f32, VadError> {
        let mut input = [0.0; CONTEXT + FRAME_SAMPLES];
        input[..CONTEXT].copy_from_slice(&self.context);
        for (dest, sample) in input[CONTEXT..].iter_mut().zip(pcm) {
            *dest = f32::from(*sample) / 32768.0;
        }
        let sample_rate = [i64::from(SAMPLE_RATE)];
        let input_tensor = TensorRef::from_array_view(([1, CONTEXT + FRAME_SAMPLES], &input[..]))
            .map_err(inference_error)?;
        let state_tensor =
            TensorRef::from_array_view(([2, 1, 128], &self.state[..])).map_err(inference_error)?;
        let rate_tensor = TensorRef::from_array_view(([0_usize; 0], &sample_rate[..]))
            .map_err(inference_error)?;
        let outputs = self
            .session
            .run(ort::inputs! {
                "input" => input_tensor,
                "state" => state_tensor,
                "sr" => rate_tensor,
            })
            .map_err(inference_error)?;
        let (shape, probabilities) = outputs["output"]
            .try_extract_tensor::<f32>()
            .map_err(inference_error)?;
        if shape[..] != [1, 1] || probabilities.len() != 1 {
            return Err(VadError::InvalidOutput("expected output shape [1, 1]"));
        }
        let probability = probabilities[0];
        if !probability.is_finite() || !(0.0..=1.0).contains(&probability) {
            return Err(VadError::InvalidOutput(
                "probability must be finite in [0, 1]",
            ));
        }
        let (shape, state) = outputs["stateN"]
            .try_extract_tensor::<f32>()
            .map_err(inference_error)?;
        if shape[..] != [2, 1, 128] || state.len() != STATE || state.iter().any(|x| !x.is_finite())
        {
            return Err(VadError::InvalidOutput(
                "expected finite stateN with shape [2, 1, 128]",
            ));
        }
        // Commit both states only after all model outputs have been validated.
        self.state.copy_from_slice(state);
        self.context
            .copy_from_slice(&input[input.len() - CONTEXT..]);
        Ok(probability)
    }

    pub fn reset(&mut self) {
        self.context.fill(0.0);
        self.state.fill(0.0);
    }
}

fn inference_error(source: ort::Error) -> VadError {
    VadError::Inference {
        source: Box::new(source),
    }
}

fn validate_contract(session: &Session) -> Result<(), VadError> {
    let input_types = [
        (
            "input",
            TensorElementType::Float32,
            &[1, (CONTEXT + FRAME_SAMPLES) as i64][..],
        ),
        ("state", TensorElementType::Float32, &[2, 1, 128][..]),
        ("sr", TensorElementType::Int64, &[][..]),
    ];
    let output_types = [("output", &[1, 1][..]), ("stateN", &[2, 1, 128][..])];
    if session.inputs.len() != input_types.len() || session.outputs.len() != output_types.len() {
        return Err(VadError::ModelContract("expected 3 inputs and 2 outputs"));
    }
    for (name, ty, shape) in input_types {
        if !session
            .inputs
            .iter()
            .any(|input| input.name == name && compatible(&input.input_type, ty, shape))
        {
            return Err(VadError::ModelContract(
                "expected input: f32[1,576], state: f32[2,1,128], sr: i64 scalar",
            ));
        }
    }
    for (name, shape) in output_types {
        if !session.outputs.iter().any(|output| {
            output.name == name
                && compatible(&output.output_type, TensorElementType::Float32, shape)
        }) {
            return Err(VadError::ModelContract(
                "expected output: f32[1,1], stateN: f32[2,1,128]",
            ));
        }
    }
    Ok(())
}

fn compatible(value: &ValueType, expected_type: TensorElementType, expected_shape: &[i64]) -> bool {
    matches!(value, ValueType::Tensor { ty, shape, .. }
        if *ty == expected_type && shape.len() == expected_shape.len()
            && shape.iter().zip(expected_shape).all(|(&actual, &expected)| actual == -1 || actual == expected))
}
