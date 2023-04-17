package sjdb;

import java.util.List;
import java.util.ArrayList;
import java.util.Iterator;

public class Estimator implements PlanVisitor {


	public Estimator() {
		// empty constructor
	}

	/* 
	 * Create output relation on Scan operator
	 *
	 * Example implementation of visit method for Scan operators.
	 */
	public void visit(Scan op) {
		Relation input = op.getRelation();
		Relation output = new Relation(input.getTupleCount());
		
		Iterator<Attribute> iter = input.getAttributes().iterator();
		while (iter.hasNext()) {
			output.addAttribute(new Attribute(iter.next()));
		}
		
		op.setOutput(output);
	}

	public void visit(Project op) {
		/*
		 `getInput`: get the Operator of Project
		 `getOutput`: get related output of an Operator
		 */
		Relation input = op.getInput().getOutput();
		Relation output = new Relation(input.getTupleCount());

		Iterator<Attribute> iter = op.getAttributes().iterator();
		while (iter.hasNext()) {
			// get the Attribute exactly from Operator
			output.addAttribute(new Attribute(input.getAttribute(iter.next())));
		}

		op.setOutput(output);

	}
	
	public void visit(Select op) {
		Relation input = op.getInput().getOutput();
		Relation output;
		Iterator<Attribute> iter = input.getAttributes().iterator();
		int value;

		Predicate predicate = op.getPredicate();
		if (predicate.equalsValue()) {
			// attr=value
			Attribute leftAttribute = predicate.getLeftAttribute();
			String rightValue = predicate.getRightValue();

			//T(σA=c(R)) = T(R)/V(R,A)
			output = new Relation(input.getTupleCount() / leftAttribute.getValueCount());
			// V(σA=c(R),A) = 1
			value = 1;

			while (iter.hasNext()) {
				Attribute newAttribute = iter.next().equals(leftAttribute) ?
						new Attribute(leftAttribute.getName(), value) :
						new Attribute(iter.next());
				output.addAttribute(newAttribute);
			}
		} else {
			// attr=attr
			Attribute leftAttribute = predicate.getLeftAttribute();
			Attribute rightAttribute = predicate.getRightAttribute();

			// T(σA=B(R)) = T(R)/max(V(R,A),V(R,B))
			output = new Relation(input.getTupleCount() / Math.max(leftAttribute.getValueCount(), rightAttribute.getValueCount()));
			// V(σA=B(R),A) = V(σA=B(R),B) = min(V(R,A), V(R,B)
			value = Math.min(leftAttribute.getValueCount(), rightAttribute.getValueCount());

			while (iter.hasNext()) {
				Attribute newAttribute = iter.next().equals(leftAttribute) ?
						new Attribute(leftAttribute.getName(), value) :
						iter.next().equals(leftAttribute) ?
								new Attribute(rightAttribute.getName(), value) :
								new Attribute(iter.next());
				output.addAttribute(newAttribute);
			}
		}

		op.setOutput(output);
	}
	
	public void visit(Product op) {
		// T(R × S) = T(R)T(S)
		Relation leftInput = op.getLeft().getOutput();
		Relation rightInput = op.getRight().getOutput();

		Relation output = new Relation(leftInput.getTupleCount() * rightInput.getTupleCount());

		Iterator<Attribute> iterLeft = leftInput.getAttributes().iterator();
		Iterator<Attribute> iterRight = rightInput.getAttributes().iterator();
		while (iterLeft.hasNext()) {
			output.addAttribute(new Attribute(iterLeft.next()));
		}
		while (iterRight.hasNext()){
			output.addAttribute(new Attribute(iterRight.next()));
		}

		op.setOutput(output);
	}
	
	public void visit(Join op) {
		// T(R ⋈ S) = T(R)T(S)/max(V(R,A),V(S,B))
		Relation leftInput = op.getLeft().getOutput();
		Relation rightInput = op.getRight().getOutput();

		Predicate predicate = op.getPredicate();
		Attribute leftAttribute = predicate.getLeftAttribute();
		Attribute rightAttribute = predicate.getRightAttribute();
		int value = Math.min(leftAttribute.getValueCount(), rightAttribute.getValueCount());

		// V(R⨝A=BS,A) = V(R⨝A=BS,B) = min(V(R,A), V(S,B))
		Relation output = new Relation(leftInput.getTupleCount() * rightInput.getTupleCount() /
				Math.max(leftAttribute.getValueCount(), rightAttribute.getValueCount()));

		Iterator<Attribute> iterLeft = leftInput.getAttributes().iterator();
		Iterator<Attribute> iterRight = rightInput.getAttributes().iterator();
		while (iterLeft.hasNext()) {
			Attribute newAttribute = iterLeft.next().equals(leftAttribute) ?
					new Attribute(leftAttribute.getName(), value) :
					new Attribute(iterLeft.next());
			output.addAttribute(newAttribute);
		}
		while (iterRight.hasNext()){
			Attribute newAttribute = iterRight.next().equals(rightAttribute) ?
					new Attribute(rightAttribute.getName(), value) :
					new Attribute(iterRight.next());
			output.addAttribute(newAttribute);
		}

		op.setOutput(output);
	}
}
